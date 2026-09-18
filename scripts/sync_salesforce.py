#!/usr/bin/env python3
"""
Sync Salesforce Leads / Opportunities / Campaign Members for an event's booth-scan
campaigns into flat JSON files under an output directory, so they can be read by
other tools (e.g. Claude via the GitHub connector) without ever needing direct
Salesforce API access.

This script is EVENT-GENERIC: everything specific to one event/campaign (which
Campaign Ids to pull, which reps own the pipeline, the event's start date, the
Lead Source value that marks an Opportunity as sourced from this event, the
comparison campaign for a year-over-year view, the Account custom field for ABM
tier, etc.) lives in a small JSON config file, not in this file. To stand up a
new event, copy events/<name>/config.json from an existing one, adjust the
values, and point EVENT_CONFIG at it -- no code changes required.

Auth: OAuth 2.0 "Client Credentials" flow, via a Salesforce External Client
App (the newer replacement for classic Connected Apps). This flow only needs
a Client ID + Client Secret -- no username, password, or security token.
Credentials are read ONLY from environment variables (populated by GitHub
Actions secrets) -- never hardcoded, never logged.

Required env vars:
  SF_LOGIN_URL       your org's My Domain login URL, e.g.
                      https://yourorg.my.salesforce.com
                      (use the sandbox My Domain URL for a sandbox org)
  SF_CLIENT_ID       External Client App Consumer Key
  SF_CLIENT_SECRET   External Client App Consumer Secret

Optional env var:
  EVENT_CONFIG       path (relative to the repo root) to this run's event
                      config JSON. Defaults to events/raise2026/config.json
                      for backward compatibility with the original RAISE 2026
                      setup.

Note: Client Credentials Flow authenticates as whatever "Run As" user is
configured under the External Client App's Policies tab -> Client
Credentials Flow section -- that user's permissions determine what data this
script can see, so make sure it's a user with access to the relevant
Leads/Opportunities. SF_LOGIN_URL must be the org's specific My Domain URL
for this flow (the generic https://login.salesforce.com will not work for
Client Credentials Flow).
"""
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib import request, parse, error

REPO_ROOT = Path(__file__).resolve().parent.parent

SF_LOGIN_URL = os.environ["SF_LOGIN_URL"]
CLIENT_ID = os.environ["SF_CLIENT_ID"]
CLIENT_SECRET = os.environ["SF_CLIENT_SECRET"]

# --- Event config: everything specific to ONE event/campaign lives here, not
# hardcoded in this script, so the exact same sync logic can be reused for a
# different event just by pointing EVENT_CONFIG at a different file.
CONFIG_PATH = REPO_ROOT / os.environ.get("EVENT_CONFIG", "events/raise2026/config.json")
CONFIG = json.loads(CONFIG_PATH.read_text())

CAMPAIGN_IDS = CONFIG["campaign_ids"]
REP_NAMES = CONFIG["rep_names"]
DEFAULT_OPP_AMOUNT = CONFIG["default_opp_amount"]
QUALIFIED_STAGES = set(CONFIG["qualified_stages"])  # matches dashboard's "Qualified" definition

# The prior event's directly-comparable in-person booth campaign (booth-only
# apples-to-apples comparison, per user direction -- NOT necessarily the full
# prior event, which may also have separate Sponsors/Attendees/Speakers/
# On-Site-Meetings/Virtual-Booth campaigns under the same parent Campaign).
COMPARISON_CAMPAIGN_ID = CONFIG.get("comparison_campaign_id")  # optional: None for a brand-new event with no prior-year campaign to compare against

# Event start date. Used to distinguish an Opportunity that is genuinely NEW
# because of this event from one that already existed in the pipeline and
# merely happens to match a booth-scan account by domain.
EVENT_START_DATE = CONFIG["event_start_date"]

# Exact Lead Source value reps tag on Opportunities sourced from this event.
NET_NEW_LEAD_SOURCE = CONFIG["net_new_lead_source"]

# Account custom field holding this org's ABM tier classification (e.g. Tier
# 1-4). Confirm the exact API name with the user before changing -- it varies
# per Salesforce org.
ABM_TIER_FIELD = CONFIG.get("abm_tier_field", "ABM_Tier__c")

AD_HOC_REPORT_ID = CONFIG.get("ad_hoc_report_id")

# Additional one-off report ids for the Booth Leads & AE Pipeline tab
# (aiinfra-2026-sales-priorities.html): 4 AE/pipeline reports + 4 contact
# reports. Fetched the exact same non-fatal way as AD_HOC_REPORT_ID, just
# looped -- see fetch_report() and the ad-hoc pull at the bottom of main().
ADDITIONAL_REPORT_IDS = list(CONFIG.get("ae_pipeline_report_ids", [])) + \
    list(CONFIG.get("contact_report_ids", []))

# Optional: path (relative to repo root) to a static JSON roster of this
# event's attendees/speakers -- [{ "company": "...", "people": [{"first_name",
# "last_name", "title", "email"}, ...] }, ...]. When set, this script
# cross-references that roster against a LIVE, uncapped pull of ALL
# ABM-tiered Accounts (not just the ones matched to booth-scan Leads) plus
# their open Opportunities, reproducing a "pre-event tier mapping +
# opportunity attribution" analysis on every sync run instead of a one-off
# manual Excel export. The roster itself is static (re-upload a new file and
# commit it if the attendee list changes) but the tier/opportunity data next
# to it is refreshed every run. Skipped entirely (non-fatal) if unset or the
# file is missing, so other events are unaffected.
ATTENDEE_ROSTER_PATH = CONFIG.get("attendee_roster_path")

# Org's Lightning "My Domain" base URL, used to build Account-level record
# links for the pre-event tier mapping table (Opportunity/Lead links elsewhere
# in this script already use instance_url from the OAuth response, which
# points at the *.my.salesforce.com host -- functionally equivalent, but this
# constant matches the exact Lightning domain requested for Account links).
SFDC_LIGHTNING_DOMAIN = CONFIG.get("sfdc_lightning_domain", "https://clockworksystems.lightning.force.com")

OUT_DIR = REPO_ROOT / CONFIG.get("output_dir", "data")


def get_access_token():
    url = f"{SF_LOGIN_URL}/services/oauth2/token"
    payload = parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }).encode()
    req = request.Request(url, data=payload, method="POST")
    try:
        with request.urlopen(req) as resp:
            data = json.loads(resp.read())
            return data["access_token"], data["instance_url"]
    except error.HTTPError as e:
        print("Auth failed:", e.read().decode(), file=sys.stderr)
        raise


def soql(instance_url, token, query):
    records = []
    url = f"{instance_url}/services/data/v60.0/query/?q={parse.quote(query)}"
    while url:
        req = request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with request.urlopen(req) as resp:
                data = json.loads(resp.read())
        except error.HTTPError as e:
            print("SOQL query failed:", e.read().decode(), file=sys.stderr)
            print("Query was:", query, file=sys.stderr)
            raise
        records.extend(data["records"])
        next_url = data.get("nextRecordsUrl")
        url = f"{instance_url}{next_url}" if next_url else None
    return records


def fetch_report(instance_url, token, report_id):
    """Fetch a saved Salesforce Report's data via the Analytics/Reports REST
    API (read-only). Returns the raw report JSON (reportMetadata + factMap +
    groupings), whose shape depends on the report's format (tabular, summary,
    or matrix) -- exploratory, so callers should inspect the result before
    assuming a specific structure. Returns None (non-fatal) if the API user
    lacks Reports API access or the report doesn't exist/isn't visible to it."""
    url = f"{instance_url}/services/data/v60.0/analytics/reports/{report_id}"
    req = request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with request.urlopen(req) as resp:
            return json.loads(resp.read())
    except error.HTTPError as e:
        print(f"Report fetch failed for {report_id}:", e.read().decode(), file=sys.stderr)
        return None


def domain_of(url):
    """Normalize a website URL down to a bare registrable-ish domain for
    matching Leads to Opportunities/Accounts, e.g.
    'https://www.Acme.com/about' -> 'acme.com'. Returns None for blank input."""
    if not url:
        return None
    u = url.strip().lower()
    u = re.sub(r"^[a-z]+://", "", u)   # strip scheme
    u = re.sub(r"^www\.", "", u)       # strip leading www.
    u = u.split("/")[0]                # strip path
    u = u.split("?")[0].split("#")[0]  # strip query/fragment (belt & suspenders)
    u = u.split(":")[0]                # strip port
    return u or None


# --- Company-name normalization for matching a static attendee roster (which
# has no domain to key off of) against Salesforce Account.Name. Strips
# punctuation and the most common legal-entity/business-word suffixes so
# "Acme, Inc." and "Acme" both normalize to "acme". This is inherently
# fuzzier than the domain-based matching used elsewhere in this script --
# treat mismatches as a starting point for manual review, not ground truth.
_COMPANY_SUFFIX_RE = re.compile(
    r"\b(inc|llc|ltd|corp|corporation|co|company|group|technologies|"
    r"technology|labs|lab|systems|holdings|international|the)\b"
)


def normalize_company_name(name):
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r"[.,]", "", n)
    n = _COMPANY_SUFFIX_RE.sub("", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


# --- Lightly groups a handful of known raw Campaign Names into a shared
# "event family" label, purely for readability in the pre-event Opportunity
# Attribution view. This is a hand-maintained pattern list, not a Salesforce
# field -- extend it as new named campaigns show up tied to attending
# accounts' Opportunities.
_EVENT_FAMILY_RULES = [
    (re.compile(r"raise\s*2025", re.I), "RAISE 2025 (Conference)"),
    (re.compile(r"raise\s*2026", re.I), "RAISE 2026 (Conference)"),
    (re.compile(r"pytorch\s*2025", re.I), "PyTorch 2025 (Conference)"),
    (re.compile(r"pytorch\s*2026", re.I), "PyTorch 2026 (Conference)"),
    (re.compile(r"ai infra summit\s*2025", re.I), "AI Infra Summit 2025 (Conference)"),
    (re.compile(r"ai infra summit\s*2026", re.I), "AI Infra Summit 2026 (Conference)"),
    (re.compile(r"webinar", re.I), "Webinar"),
]


def classify_event_family(campaign_name):
    if not campaign_name:
        return None
    for rx, label in _EVENT_FAMILY_RULES:
        if rx.search(campaign_name):
            return label
    return None


def classify_lifecycle(status, notes):
    """Best-effort MQL/MEL classification when Status doesn't already reflect it.
    Falls back to reading free-text Notes for qualification signals.
    Treat this as a starting point -- review before relying on it."""
    if status and status.lower() not in ("lead", "open", "new", ""):
        return status
    text = (notes or "").lower()
    qualified_signals = ["interested", "follow up", "follow-up", "demo", "budget",
                          "pilot", "poc", "evaluating", "buying", "timeline", "qualified"]
    if any(sig in text for sig in qualified_signals):
        return "Marketing Qualified Lead"
    return "Marketing Engaged Lead"


def esc(s):
    return s.replace("\\", "\\\\").replace("'", "\\'")


def is_open_stage(stage):
    """An Opportunity stage counts as still-open (live) pipeline unless it's
    a Closed Won or Closed Lost terminal stage. Matched loosely on the word
    'Closed' since orgs vary in exact stage-name spelling."""
    return "closed" not in (stage or "").lower()


def opp_origin(created_date, lead_source):
    """Classify an Opportunity as "Net New" (genuinely sourced from this
    event) vs "Existing" (a pre-existing pipeline deal).

    Existing = Created Date on or before the event's start date, for a
    domain-matched account -- true regardless of Lead Source, stage, or
    whether it's since closed. A deal that was already Closed Lost, or
    already deep into later stages, obviously predates a booth conversation
    that happened only days ago.

    Net New requires ALL of:
      (a) Lead Source exactly matches NET_NEW_LEAD_SOURCE (not a generic
          "Event" tag, which could reflect any past event/conference), AND
      (b) Created Date is on/after the event's start date, AND
      (c) it's already domain-matched to a booth-scan account (guaranteed by
          the domain-match filter this runs inside of).

    "Day 0" opportunities are expected and valid: a booth conversation can
    be extensive enough to justify creating an Opportunity straight at a
    later stage (skipping earlier lead stages), or to progress quickly
    through stages post-event. The one thing that's NOT plausible for a
    genuine Net New deal is already being Closed Lost within days of the
    event -- but that's naturally excluded here since it would require both
    the exact NET_NEW_LEAD_SOURCE tag AND a created date in-window, which a
    truly pre-existing/lost deal won't have.
    """
    if not created_date:
        return "Existing"
    created_day = created_date[:10]  # ISO datetime "YYYY-MM-DDTHH:MM:SS..." -> date prefix
    if created_day >= EVENT_START_DATE and lead_source == NET_NEW_LEAD_SOURCE:
        return "Net New"
    return "Existing"


def pull_campaign_leads(instance_url, token, campaign_ids):
    """Pull CampaignMember->Lead rows for a list of Campaign Ids, shaped
    identically for both the 2026 booth-scan campaigns and the 2025
    comparison campaign."""
    id_list = ",".join(f"'{c}'" for c in campaign_ids)
    cm_query = f"""
        SELECT CampaignId, Campaign.Name, Status, LeadId, CreatedDate,
               Lead.OwnerId, Lead.Owner.Name, Lead.Company, Lead.Website,
               Lead.FirstName, Lead.LastName, Lead.Title, Lead.Email,
               Lead.MobilePhone, Lead.LeadSource, Lead.Status,
               Lead.Lead_Notes__c, Lead.LinkedIn__c, Lead.LastActivityDate
        FROM CampaignMember
        WHERE CampaignId IN ({id_list}) AND LeadId != null
    """
    members = soql(instance_url, token, cm_query)

    out = []
    for m in members:
        lead = m.get("Lead") or {}
        notes = lead.get("Lead_Notes__c")
        status = lead.get("Status")
        out.append({
            "campaign": (m.get("Campaign") or {}).get("Name"),
            "member_status": m.get("Status"),
            "lead_id": m.get("LeadId"),
            "owner": (lead.get("Owner") or {}).get("Name"),
            "company": lead.get("Company"),
            "website": lead.get("Website"),
            "domain": domain_of(lead.get("Website")),
            "first_name": lead.get("FirstName"),
            "last_name": lead.get("LastName"),
            "title": lead.get("Title"),
            "email": lead.get("Email"),
            "mobile": lead.get("MobilePhone"),
            "linkedin_url": lead.get("LinkedIn__c"),
            "lead_source": lead.get("LeadSource"),
            "notes": notes,
            "lifecycle_stage": classify_lifecycle(status, notes),
            "created_date": m.get("CreatedDate"),
            "last_activity_date": lead.get("LastActivityDate"),
            "sfdc_link": f"{instance_url}/lightning/r/Lead/{m.get('LeadId')}/view",
        })
    return out


def pull_all_tiered_accounts(instance_url, token, tier_field):
    """Live, uncapped pull of EVERY Account that has an ABM tier set,
    regardless of whether it's tied to any booth-scan Lead. Unlike the
    Salesforce Reports API (capped at 2,000 detail rows per run), a plain
    SOQL query() paginates via nextRecordsUrl with no such cap, so this sees
    the full tiered-account universe. Returns [] (non-fatal) if the query
    fails, e.g. a field name here doesn't exist in this org."""
    query = f"""
        SELECT Id, Name, OwnerId, Owner.Name, {tier_field},
               Industry, GPU_Segment__c, Target_Account__c, ICP_Account__c
        FROM Account
        WHERE {tier_field} != null
    """
    try:
        return soql(instance_url, token, query)
    except error.HTTPError:
        print("Tiered-account pull failed (non-fatal) -- pre-event analysis will be skipped.",
              file=sys.stderr)
        return []


def pull_opportunities_for_accounts(instance_url, token, account_ids):
    """Pull ALL Opportunities (any stage, any owner) for a specific, bounded
    set of Account Ids -- used for the pre-event tier-mapping analysis so
    open-opportunity attribution isn't limited to accounts that happen to
    have a Website populated (unlike the domain-matching Opportunity pull
    used elsewhere in this script)."""
    if not account_ids:
        return []
    id_list = ",".join(f"'{esc(i)}'" for i in account_ids)
    query = f"""
        SELECT Id, Name, AccountId, OwnerId, Owner.Name, Amount, StageName, Type,
               CloseDate, LeadSource, CampaignId, Campaign.Name,
               CreatedDate, LastActivityDate,
               (SELECT Contact.Name, Contact.Title, Contact.Email FROM OpportunityContactRoles)
        FROM Opportunity
        WHERE AccountId IN ({id_list})
    """
    return soql(instance_url, token, query)


def build_pre_event_analysis(instance_url, token, roster_path, tier_field):
    """Cross-references a static attendee/speaker roster against a live,
    uncapped pull of ABM-tiered Accounts and their open Opportunities.
    Reproduces (approximately -- see the inline notes below) the kind of
    manual "Tier Mapping & Opportunity Attribution" analysis that would
    otherwise require exporting three separate Salesforce reports and
    joining them by hand in Excel. Returns None (non-fatal) if the roster
    file is missing or the tiered-account pull fails."""
    roster_file = REPO_ROOT / roster_path
    if not roster_file.exists():
        print(f"Attendee roster not found at {roster_file} -- skipping pre-event analysis.",
              file=sys.stderr)
        return None
    roster = json.loads(roster_file.read_text())

    accounts = pull_all_tiered_accounts(instance_url, token, tier_field)
    if not accounts:
        return None

    accounts_by_norm = {}
    for a in accounts:
        norm = normalize_company_name(a.get("Name"))
        if norm and norm not in accounts_by_norm:  # first tiered account wins on a name collision
            accounts_by_norm[norm] = a

    matched_accounts = []
    unmatched_companies = []
    matched_account_ids = []

    for entry in roster:
        company = entry.get("company")
        people = entry.get("people", [])
        norm = normalize_company_name(company)
        acct = accounts_by_norm.get(norm)
        attendees = [
            {"name": f"{p.get('first_name') or ''} {p.get('last_name') or ''}".strip(),
             "title": p.get("title")}
            for p in people
        ]
        if acct:
            matched_account_ids.append(acct["Id"])
            matched_accounts.append({
                "tier": acct.get(tier_field) or "Untiered",
                "company": acct.get("Name"),
                "account_id": acct["Id"],
                "account_sfdc_link": f"{SFDC_LIGHTNING_DOMAIN}/lightning/r/Account/{acct['Id']}/view",
                "owner": (acct.get("Owner") or {}).get("Name"),
                "gpu_segment": acct.get("GPU_Segment__c"),
                "industry": acct.get("Industry"),
                "target_account": bool(acct.get("Target_Account__c")),
                "icp_account": acct.get("ICP_Account__c"),
                "attendee_count": len(people),
                "attendees": attendees,
                # filled in below once open-opportunity data is joined in
                "has_open_opportunity": False,
                "stage": "No Open Opportunity",
                "amount": None,
                "best_contact": None,
                "best_contact_title": None,
                "best_contact_email": None,
                "primary_campaign": None,
                "event_family": None,
                "first_touch": None,
                "last_touch": None,
                "opp_id": None,
                "sfdc_link": None,
            })
        else:
            unmatched_companies.append({
                "company": company,
                "attendee_count": len(people),
                "attendees": attendees,
            })

    # --- Open-opportunity attribution, scoped to just the matched accounts.
    # "Open" here means: not a Closed Won/Lost stage, Type doesn't look like
    # a renewal, and Close Date falls in the current calendar year -- a
    # best-effort reproduction of a typical "open pipeline this year, new
    # business only" saved-report filter. If your org's actual saved report
    # (e.g. a "Revenue Intel" style export) uses different filters, treat
    # this as an approximation and reconcile against that report directly.
    all_opps = pull_opportunities_for_accounts(instance_url, token, matched_account_ids)
    current_year = str(datetime.now(timezone.utc).year)
    open_opps_by_account = {}
    for o in all_opps:
        stage = o.get("StageName") or ""
        if not is_open_stage(stage):
            continue
        if "renewal" in (o.get("Type") or "").lower():
            continue
        close_date = o.get("CloseDate") or ""
        if not close_date.startswith(current_year):
            continue
        acct_id = o.get("AccountId")
        if acct_id:
            open_opps_by_account.setdefault(acct_id, []).append(o)

    tier_breakdown = {}
    source_attribution = {}
    opportunity_rows = []
    total_open_pipeline = 0
    accounts_with_open_opp = 0
    opps_tied_to_named_campaign = 0

    for row in matched_accounts:
        tier = row["tier"]
        tier_breakdown.setdefault(tier, {"accounts": 0, "accounts_with_opp": 0, "open_pipeline": 0})
        tier_breakdown[tier]["accounts"] += 1

        acct_opps = open_opps_by_account.get(row["account_id"], [])
        if acct_opps:
            accounts_with_open_opp += 1
            tier_breakdown[tier]["accounts_with_opp"] += 1
            acct_pipeline = sum(o.get("Amount") or 0 for o in acct_opps)
            tier_breakdown[tier]["open_pipeline"] += acct_pipeline
            total_open_pipeline += acct_pipeline

            best_opp = max(acct_opps, key=lambda o: o.get("Amount") or 0)
            best_contact = None
            best_contact_title = None
            best_contact_email = None
            roles = (best_opp.get("OpportunityContactRoles") or {}).get("records", [])
            if roles:
                best_role_contact = roles[0].get("Contact") or {}
                best_contact = best_role_contact.get("Name")
                best_contact_title = best_role_contact.get("Title")
                best_contact_email = best_role_contact.get("Email")

            campaign_name = (best_opp.get("Campaign") or {}).get("Name")

            row.update({
                "has_open_opportunity": True,
                "stage": best_opp.get("StageName"),
                "amount": best_opp.get("Amount"),
                "best_contact": best_contact,
                "best_contact_title": best_contact_title,
                "best_contact_email": best_contact_email,
                "primary_campaign": campaign_name,
                "event_family": classify_event_family(campaign_name),
                "first_touch": best_opp.get("CreatedDate"),
                "last_touch": best_opp.get("LastActivityDate"),
                "opp_id": best_opp.get("Id"),
                "sfdc_link": f"{instance_url}/lightning/r/Opportunity/{best_opp.get('Id')}/view",
            })

            for o in acct_opps:
                lead_source = o.get("LeadSource") or "Not Set"
                source_attribution.setdefault(lead_source, {"opportunities": 0, "pipeline": 0})
                source_attribution[lead_source]["opportunities"] += 1
                source_attribution[lead_source]["pipeline"] += o.get("Amount") or 0

                campaign_name = (o.get("Campaign") or {}).get("Name")
                event_family = classify_event_family(campaign_name)
                if campaign_name:
                    opps_tied_to_named_campaign += 1

                opportunity_rows.append({
                    "account": row["company"],
                    "tier": tier,
                    "owner": row["owner"],
                    "opportunity_name": o.get("Name"),
                    "stage": o.get("StageName"),
                    "amount": o.get("Amount"),
                    "close_date": o.get("CloseDate"),
                    "lead_source": lead_source,
                    "primary_campaign_raw": campaign_name,
                    "event_family": event_family,
                    "opp_id": o.get("Id"),
                    "sfdc_link": f"{instance_url}/lightning/r/Opportunity/{o.get('Id')}/view",
                })

        del row["account_id"]  # internal-only join key, not needed downstream

    matched_accounts.sort(key=lambda a: (a["tier"], -(a["amount"] or 0)))
    unmatched_companies.sort(key=lambda u: -u["attendee_count"])
    opportunity_rows.sort(key=lambda o: -(o["amount"] or 0))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "attending_accounts": len(matched_accounts),
            "unmatched_companies": len(unmatched_companies),
            "accounts_with_open_opportunity": accounts_with_open_opp,
            "open_pipeline_total": total_open_pipeline,
            "opportunities_tied_to_named_campaign": opps_tied_to_named_campaign,
            "tier_breakdown": tier_breakdown,
            "source_attribution": source_attribution,
        },
        "accounts": matched_accounts,
        "unmatched_companies": unmatched_companies,
        "opportunities": opportunity_rows,
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    token, instance_url = get_access_token()

    # --- Pull both years' booth-scan leads up front, so engagement detection
    # below can correctly exclude EACH year's own attendees (not just 2026's)
    # when looking for "some other Lead already at this company" evidence.
    leads_out = pull_campaign_leads(instance_url, token, CAMPAIGN_IDS)
    leads_2025 = pull_campaign_leads(instance_url, token, [COMPARISON_CAMPAIGN_ID]) if COMPARISON_CAMPAIGN_ID else []

    lead_domains = {l["domain"] for l in leads_out if l.get("domain")}
    domains_2025 = {l["domain"] for l in leads_2025 if l.get("domain")}

    booth_scan_lead_ids = {l["lead_id"] for l in leads_out if l.get("lead_id")}
    lead_ids_2025 = {l["lead_id"] for l in leads_2025 if l.get("lead_id")}
    all_booth_lead_ids = booth_scan_lead_ids | lead_ids_2025

    # --- Opportunities: pulled for ALL owners (not just the 4 reps) so we can
    # detect "this account already has SOME opportunity, even if a different
    # rep/team owns it" for the company-level engagement flag below. The
    # rep-specific pipeline list (opps_out) is then filtered down to just the
    # 4 reps from this same result set. Matched to booth-scan Leads by WEBSITE
    # DOMAIN rather than Account.Name (fuzzy text) or CampaignId (most Opps
    # won't be tagged with the campaign directly).
    opp_query = """
        SELECT Id, OwnerId, Owner.Name, AccountId, Account.Name, Account.Website,
               Amount, StageName, LeadSource, CreatedDate, CampaignId, Campaign.Name,
               (SELECT Contact.Name, Contact.Title, Contact.Email FROM OpportunityContactRoles)
        FROM Opportunity
        WHERE Account.Website != null
    """
    all_opps = soql(instance_url, token, opp_query)

    opps_out = []
    company_engaged_via_opp = set()       # domains from 2026 lead set
    company_engaged_via_opp_2025 = set()  # domains from 2025 lead set
    # ALL-owner opportunities by domain (2026 booth-company domains only) --
    # used for the account-level "open opportunity" pipeline calc below, so a
    # real deal owned outside the 4 tracked reps still counts with its real
    # $ and stage instead of being silently dropped/estimated.
    opps_all_owners_by_domain = {}
    for o in all_opps:
        acct = o.get("Account") or {}
        opp_domain = domain_of(acct.get("Website"))
        if not opp_domain:
            continue
        owner_name = (o.get("Owner") or {}).get("Name")
        if opp_domain in lead_domains:
            company_engaged_via_opp.add(opp_domain)  # any owner counts as "already engaged"
        if opp_domain in domains_2025:
            company_engaged_via_opp_2025.add(opp_domain)
        if opp_domain in lead_domains:
            stage = o.get("StageName")
            opps_all_owners_by_domain.setdefault(opp_domain, []).append({
                "opp_id": o.get("Id"),
                "owner": owner_name,
                "account": acct.get("Name"),
                "stage": stage,
                "amount": o.get("Amount") or DEFAULT_OPP_AMOUNT,
                "is_open": is_open_stage(stage),
                "sfdc_link": f"{instance_url}/lightning/r/Opportunity/{o.get('Id')}/view",
            })
        if opp_domain in lead_domains and owner_name in REP_NAMES:
            roles = (o.get("OpportunityContactRoles") or {}).get("records", [])
            opps_out.append({
                "opp_id": o.get("Id"),
                "owner": owner_name,
                "account": acct.get("Name"),
                "account_website": acct.get("Website"),
                "domain": opp_domain,
                "amount": o.get("Amount") or DEFAULT_OPP_AMOUNT,
                "stage": o.get("StageName"),
                "is_open": is_open_stage(o.get("StageName")),
                "lead_source": o.get("LeadSource"),
                "created_date": o.get("CreatedDate"),
                "origin": opp_origin(o.get("CreatedDate"), o.get("LeadSource")),
                "campaign": (o.get("Campaign") or {}).get("Name"),
                "contacts": [
                    {"name": (r.get("Contact") or {}).get("Name"),
                     "title": (r.get("Contact") or {}).get("Title"),
                     "email": (r.get("Contact") or {}).get("Email")}
                    for r in roles
                ],
                "sfdc_link": f"{instance_url}/lightning/r/Opportunity/{o.get('Id')}/view",
            })

    # --- Accounts: pull the ABM_Tier__c field directly from the Account
    # object (Leads aren't linked to Account until converted, so tier can't
    # be read off the Lead/CampaignMember rows above -- it has to come from
    # here, joined back onto the booth-scan domains the same way everything
    # else is: by website domain).
    account_query = f"""
        SELECT Id, Name, Website, {ABM_TIER_FIELD}, Industry, GPU_Segment__c
        FROM Account
        WHERE Website != null
    """
    all_accounts = soql(instance_url, token, account_query)
    domain_to_tier = {}
    domain_to_account = {}
    for a in all_accounts:
        d = domain_of(a.get("Website"))
        if not d:
            continue
        if a.get(ABM_TIER_FIELD):
            domain_to_tier[d] = a.get(ABM_TIER_FIELD)
        # First Account seen for a domain wins on a collision, same
        # convention as accounts_by_norm in build_pre_event_analysis().
        domain_to_account.setdefault(d, a)

    # --- Contacts: any Contact already sitting on one of these Accounts is a
    # strong "someone here is already a known relationship" signal, regardless
    # of who owns the Account.
    contact_query = """
        SELECT Id, Name, Title, Email, AccountId, Account.Name, Account.Website
        FROM Contact
        WHERE Account.Website != null
    """
    contacts = soql(instance_url, token, contact_query)

    company_engaged_via_contact = set()
    company_engaged_via_contact_2025 = set()
    for c in contacts:
        acct = c.get("Account") or {}
        d = domain_of(acct.get("Website"))
        if not d:
            continue
        if d in lead_domains:
            company_engaged_via_contact.add(d)
        if d in domains_2025:
            company_engaged_via_contact_2025.add(d)

    # --- Other Leads: any OTHER open (unconverted) Lead at the same domain,
    # excluding BOTH years' own booth-scan Leads, means someone else from that
    # company is already a separate active thread with us. Excluding both
    # years (not just 2026) avoids each year's attendees being mistaken for
    # "outside" evidence of engagement against themselves or each other.
    company_engaged_via_other_lead = set()
    company_engaged_via_other_lead_2025 = set()
    if all_booth_lead_ids:
        exclude_ids = ",".join(f"'{esc(i)}'" for i in all_booth_lead_ids)
        other_lead_query = f"""
            SELECT Id, Company, Website, Status, OwnerId, Owner.Name, CreatedDate
            FROM Lead
            WHERE Website != null AND IsConverted = false
                  AND Id NOT IN ({exclude_ids})
        """
        other_leads = soql(instance_url, token, other_lead_query)
        for ol in other_leads:
            d = domain_of(ol.get("Website"))
            if not d:
                continue
            if d in lead_domains:
                company_engaged_via_other_lead.add(d)
            if d in domains_2025:
                company_engaged_via_other_lead_2025.add(d)

    company_already_engaged = (company_engaged_via_opp
                                | company_engaged_via_contact
                                | company_engaged_via_other_lead)
    company_already_engaged_2025 = (company_engaged_via_opp_2025
                                     | company_engaged_via_contact_2025
                                     | company_engaged_via_other_lead_2025)

    # Stamp each booth-scan lead with the company-level engagement flag.
    for l in leads_out:
        d = l.get("domain")
        l["company_already_engaged"] = "Yes" if (d and d in company_already_engaged) else "No"
    for l in leads_2025:
        d = l.get("domain")
        l["company_already_engaged"] = "Yes" if (d and d in company_already_engaged_2025) else "No"

    # --- Account-level (ABM/ABX) rollup: one row per company seen at the
    # booth, tracking BOTH the breadth (how many leads/personas we're adding)
    # and the depth (funnel stage of each, plus whether the account already
    # had a relationship before RAISE).
    by_domain = {}
    for l in leads_out:
        d = l.get("domain") or f"__no_domain__:{l.get('company')}"
        by_domain.setdefault(d, []).append(l)

    account_summary = []
    for d, group in sorted(by_domain.items(), key=lambda kv: kv[0]):
        companies = {g.get("company") for g in group if g.get("company")}
        stage_counts = {}
        for g in group:
            stage = g.get("lifecycle_stage") or "Unknown"
            stage_counts[stage] = stage_counts.get(stage, 0) + 1

        # ALL-owner opportunities at this domain (not just the 4 tracked
        # reps), so real deal amounts owned by other teams are visible
        # instead of silently falling back to the $200K estimate.
        domain_opps = [] if d.startswith("__no_domain__:") else opps_all_owners_by_domain.get(d, [])
        open_opps = [o for o in domain_opps if o["is_open"]]
        open_opp_amount_total = sum(o["amount"] for o in open_opps)
        is_qualified = any((g.get("lifecycle_stage") in QUALIFIED_STAGES) for g in group)

        # Potential Pipeline, computed per account (per user direction):
        # every company that showed up at the RAISE 2026 booth counts as a
        # pipeline opportunity in its own right -- attendance IS the
        # opportunity, no separate lifecycle-stage qualification gate.
        # 1) if the account has ANY open (non-Closed) Opportunity from ANY
        #    owner, use the real sum of those open Opportunity amounts.
        # 2) else, use the flat $200K estimate (once per account, not per
        #    contact/persona met there).
        if open_opps:
            pipeline_amount = open_opp_amount_total
            pipeline_basis = "real_open_opportunity"
        else:
            pipeline_amount = DEFAULT_OPP_AMOUNT
            pipeline_basis = "estimated"

        account = None if d.startswith("__no_domain__:") else domain_to_account.get(d)
        campaigns = sorted({g.get("campaign") for g in group if g.get("campaign")})
        created_dates = sorted(g.get("created_date") for g in group if g.get("created_date"))
        last_activity_dates = sorted(g.get("last_activity_date") for g in group if g.get("last_activity_date"))
        fallback_owner = next((g.get("owner") for g in group if g.get("owner")), None)

        account_summary.append({
            "domain": None if d.startswith("__no_domain__:") else d,
            "company": sorted(companies)[0] if companies else group[0].get("company"),
            "booth_scan_lead_count": len(group),
            "owner": (open_opps[0]["owner"] if open_opps else fallback_owner),
            "gpu_segment": account.get("GPU_Segment__c") if account else None,
            "industry": account.get("Industry") if account else None,
            "account_sfdc_link": (f"{instance_url}/lightning/r/Account/{account['Id']}/view"
                                   if account else None),
            "campaigns": campaigns,
            # "First touch" is this domain's earliest CampaignMember add;
            # "last touch" prefers the most recent Lead activity, falling
            # back to the latest CampaignMember add when no Lead activity is
            # recorded yet.
            "first_touch": created_dates[0] if created_dates else None,
            "last_touch": (last_activity_dates[-1] if last_activity_dates
                            else (created_dates[-1] if created_dates else None)),
            "personas": [
                {"name": f"{g.get('first_name') or ''} {g.get('last_name') or ''}".strip(),
                 "title": g.get("title"),
                 "lifecycle_stage": g.get("lifecycle_stage"),
                 "owner": g.get("owner"),
                 "lead_id": g.get("lead_id"),
                 "sfdc_link": g.get("sfdc_link")}
                for g in group
            ],
            "lifecycle_stage_counts": stage_counts,
            "company_already_engaged": "Yes" if d in company_already_engaged else "No",
            "engaged_via_existing_opportunity": d in company_engaged_via_opp,
            "engaged_via_existing_contact": d in company_engaged_via_contact,
            "engaged_via_other_open_lead": d in company_engaged_via_other_lead,
            "existing_opportunities_all_owners": [
                {"opp_id": o["opp_id"], "owner": o["owner"], "account": o["account"],
                 "stage": o["stage"], "amount": o["amount"], "is_open": o["is_open"],
                 "sfdc_link": o["sfdc_link"]}
                for o in domain_opps
            ],
            "has_open_opportunity": bool(open_opps),
            "is_qualified_contact": is_qualified,
            "potential_pipeline_amount": pipeline_amount,
            "potential_pipeline_basis": pipeline_basis,
            "abm_tier": domain_to_tier.get(d),
        })

    # Rep-specific breakdown (leads + opps each rep owns)
    by_rep = {}
    for rep in REP_NAMES:
        by_rep[rep] = {
            "leads": [l for l in leads_out if l["owner"] == rep],
            "opportunities": [o for o in opps_out if o["owner"] == rep],
        }

    (OUT_DIR / "leads.json").write_text(json.dumps(leads_out, indent=2))
    (OUT_DIR / "opportunities.json").write_text(json.dumps(opps_out, indent=2))
    (OUT_DIR / "by_rep.json").write_text(json.dumps(by_rep, indent=2))
    (OUT_DIR / "account_summary.json").write_text(json.dumps(account_summary, indent=2))
    (OUT_DIR / "meta.json").write_text(json.dumps({
        "campaign_ids": CAMPAIGN_IDS,
        "rep_names": REP_NAMES,
        "default_opp_amount": DEFAULT_OPP_AMOUNT,
        "lead_count": len(leads_out),
        "opp_count": len(opps_out),
        "distinct_companies": len(account_summary),
        "companies_already_engaged": sum(1 for a in account_summary if a["company_already_engaged"] == "Yes"),
        "companies_net_new": sum(1 for a in account_summary if a["company_already_engaged"] == "No"),
        "potential_pipeline_real_open_opportunity_total": sum(
            a["potential_pipeline_amount"] for a in account_summary
            if a["potential_pipeline_basis"] == "real_open_opportunity"),
        "potential_pipeline_estimated_total": sum(
            a["potential_pipeline_amount"] for a in account_summary
            if a["potential_pipeline_basis"] == "estimated"),
        "potential_pipeline_total": sum(a["potential_pipeline_amount"] for a in account_summary),
        "abm_tier_counts": {
            tier: sum(1 for a in account_summary if a["abm_tier"] == tier)
            for tier in sorted({a["abm_tier"] for a in account_summary if a["abm_tier"]})
        },
        "accounts_without_abm_tier": sum(1 for a in account_summary if not a["abm_tier"]),
    }, indent=2))

    print(f"Wrote {len(leads_out)} leads, {len(opps_out)} rep-owned opportunities, "
          f"and {len(account_summary)} account rollups to {OUT_DIR}")

    # --- Discovery: find ALL Campaigns with "Raise" in the name (any year),
    # so we can identify the RAISE 2025 campaign(s) for a year-over-year
    # comparison without guessing IDs. This is read-only/exploratory --
    # doesn't affect the 2026 booth-scan outputs above.
    discovery_query = """
        SELECT Id, Name, StartDate, EndDate, Status, Type, NumberOfLeads,
               NumberOfConvertedLeads, NumberOfOpportunities, NumberOfWonOpportunities,
               AmountAllOpportunities, ParentId, Parent.Name
        FROM Campaign
        WHERE Name LIKE '%Raise%'
        ORDER BY StartDate ASC NULLS LAST
    """
    try:
        raise_campaigns = soql(instance_url, token, discovery_query)
    except error.HTTPError:
        raise_campaigns = []
        print("Campaign discovery query failed (non-fatal) -- see stderr above.", file=sys.stderr)

    campaigns_out = [{
        "id": c.get("Id"),
        "name": c.get("Name"),
        "start_date": c.get("StartDate"),
        "end_date": c.get("EndDate"),
        "status": c.get("Status"),
        "type": c.get("Type"),
        "number_of_leads": c.get("NumberOfLeads"),
        "number_of_converted_leads": c.get("NumberOfConvertedLeads"),
        "number_of_opportunities": c.get("NumberOfOpportunities"),
        "number_of_won_opportunities": c.get("NumberOfWonOpportunities"),
        "amount_all_opportunities": c.get("AmountAllOpportunities"),
        "parent_id": c.get("ParentId"),
        "parent_name": (c.get("Parent") or {}).get("Name"),
    } for c in raise_campaigns]

    (OUT_DIR / "campaigns_raise.json").write_text(json.dumps(campaigns_out, indent=2))
    print(f"Wrote {len(campaigns_out)} Raise-named campaigns (any year) to {OUT_DIR}/campaigns_raise.json")

    # --- RAISE 2025 vs RAISE 2026 comparison (booth-only, apples-to-apples,
    # per user direction -- NOT the full multi-campaign RAISE event for
    # either year). Compares lead/opportunity counts, exact lifecycle-stage
    # funnel (real Salesforce Status values, whatever they are), and a
    # UNIFORM pipegen potential of $200,000 per Opportunity for BOTH years
    # (not real Amounts, per explicit user direction, so the two years are
    # compared on the same yardstick). Also flags which specific leads (by
    # email) and which companies (by domain) attended/appear in BOTH years.
    opps_2025 = []
    for o in all_opps:
        acct = o.get("Account") or {}
        d = domain_of(acct.get("Website"))
        if d and d in domains_2025:
            roles = (o.get("OpportunityContactRoles") or {}).get("records", [])
            opps_2025.append({
                "opp_id": o.get("Id"),
                "owner": (o.get("Owner") or {}).get("Name"),
                "account": acct.get("Name"),
                "domain": d,
                "amount": o.get("Amount"),
                "stage": o.get("StageName"),
                "contacts": [
                    {"name": (r.get("Contact") or {}).get("Name"),
                     "title": (r.get("Contact") or {}).get("Title"),
                     "email": (r.get("Contact") or {}).get("Email")}
                    for r in roles
                ],
                "sfdc_link": f"{instance_url}/lightning/r/Opportunity/{o.get('Id')}/view",
            })

    # Same shape, for the 2026 side -- ALL owners (org-wide), matched by
    # domain, so the YoY Opportunity-stage comparison and drill-down can show
    # every real 2026 Opportunity (not just the 4 tracked reps') with a
    # Salesforce link, mirroring opps_2025 above.
    opps_2026_all = []
    for o in all_opps:
        acct = o.get("Account") or {}
        d = domain_of(acct.get("Website"))
        if d and d in lead_domains:
            roles = (o.get("OpportunityContactRoles") or {}).get("records", [])
            opps_2026_all.append({
                "opp_id": o.get("Id"),
                "owner": (o.get("Owner") or {}).get("Name"),
                "account": acct.get("Name"),
                "domain": d,
                "amount": o.get("Amount"),
                "stage": o.get("StageName"),
                "contacts": [
                    {"name": (r.get("Contact") or {}).get("Name"),
                     "title": (r.get("Contact") or {}).get("Title"),
                     "email": (r.get("Contact") or {}).get("Email")}
                    for r in roles
                ],
                "sfdc_link": f"{instance_url}/lightning/r/Opportunity/{o.get('Id')}/view",
            })

    def stage_funnel(lead_list):
        counts = {}
        for l in lead_list:
            s = l.get("lifecycle_stage") or "Unknown"
            counts[s] = counts.get(s, 0) + 1
        return counts

    def opp_stage_funnel(opp_list):
        counts = {}
        for o in opp_list:
            s = o.get("stage") or "Unknown"
            counts[s] = counts.get(s, 0) + 1
        return counts

    emails_2025 = {(l.get("email") or "").strip().lower() for l in leads_2025 if l.get("email")}
    emails_2026 = {(l.get("email") or "").strip().lower() for l in leads_out if l.get("email")}
    overlap_emails = emails_2025 & emails_2026

    overlap_leads = {
        "2025": [l for l in leads_2025 if (l.get("email") or "").strip().lower() in overlap_emails],
        "2026": [l for l in leads_out if (l.get("email") or "").strip().lower() in overlap_emails],
    }

    overlap_domains = domains_2025 & lead_domains
    overlap_companies = sorted(overlap_domains)

    overlap_opps = {
        "2025": [o for o in opps_2025 if o["domain"] in overlap_domains],
        "2026": [
            {"opp_id": o.get("Id"), "owner": (o.get("Owner") or {}).get("Name"),
             "account": (o.get("Account") or {}).get("Name"),
             "domain": domain_of((o.get("Account") or {}).get("Website")),
             "amount": o.get("Amount"), "stage": o.get("StageName")}
            for o in all_opps
            if domain_of((o.get("Account") or {}).get("Website")) in overlap_domains
        ],
    }

    comparison = {
        "methodology": ("Booth-only comparison: RAISE 2025 'Booth Visitors | Post-Conf' campaign "
                         "vs RAISE 2026's 6 in-person campaigns (3 'Booth Scans' + Machina, HumanX, "
                         "and VIP Access). Opportunities matched to booth "
                         "leads by website domain (not CampaignId, which undercounts for both years). "
                         "Pipegen potential uses a UNIFORM $200,000 per Opportunity for both years "
                         "(not real Opportunity Amounts), per explicit request, so the two years are "
                         "compared on the same yardstick."),
        "raise_2025": {
            "campaign_id": COMPARISON_CAMPAIGN_ID,
            "lead_count": len(leads_2025),
            "opportunity_count": len(opps_2025),
            "lifecycle_stage_counts": stage_funnel(leads_2025),
            "opp_stage_counts": opp_stage_funnel(opps_2025),
            "pipegen_potential": len(opps_2025) * DEFAULT_OPP_AMOUNT,
            "real_opportunity_amount_total": sum(o["amount"] or 0 for o in opps_2025),
            "opportunities": opps_2025,
        },
        "raise_2026": {
            "campaign_ids": CAMPAIGN_IDS,
            "lead_count": len(leads_out),
            "opportunity_count": len(opps_2026_all),
            "lifecycle_stage_counts": stage_funnel(leads_out),
            "opp_stage_counts": opp_stage_funnel(opps_2026_all),
            "pipegen_potential": len(opps_2026_all) * DEFAULT_OPP_AMOUNT,
            "real_opportunity_amount_total": sum(o["amount"] or 0 for o in opps_2026_all),
            "opportunities": opps_2026_all,
        },
        "overlap": {
            "overlapping_lead_emails": sorted(overlap_emails),
            "overlapping_leads_2025": overlap_leads["2025"],
            "overlapping_leads_2026": overlap_leads["2026"],
            "overlapping_companies": overlap_companies,
            "overlapping_opportunities_2025": overlap_opps["2025"],
            "overlapping_opportunities_2026": overlap_opps["2026"],
        },
    }

    (OUT_DIR / "raise2025_booth_leads.json").write_text(json.dumps(leads_2025, indent=2))
    (OUT_DIR / "year_comparison.json").write_text(json.dumps(comparison, indent=2))
    print(f"Wrote {len(leads_2025)} RAISE 2025 booth leads (with engagement flag) "
          f"and year_comparison.json to {OUT_DIR}")

    # --- One-off, exploratory pull of a specific saved Salesforce Report
    # (Report Id, "00O" prefix -- NOT a Campaign), if this event's config
    # names one. Read-only, non-fatal: dumps the raw Reports API response
    # as-is so its actual structure (tabular/summary/matrix) can be inspected
    # before any further processing is built around it.
    for report_id in ([AD_HOC_REPORT_ID] if AD_HOC_REPORT_ID else []) + ADDITIONAL_REPORT_IDS:
        report_data = fetch_report(instance_url, token, report_id)
        if report_data is not None:
            (OUT_DIR / f"report_{report_id}.json").write_text(json.dumps(report_data, indent=2))
            print(f"Wrote raw Report {report_id} data to {OUT_DIR}/report_{report_id}.json")
        else:
            print(f"Report {report_id} fetch failed or unavailable (see stderr above) -- non-fatal.", file=sys.stderr)

    # --- Pre-event tier mapping + opportunity attribution, live-reproduced
    # from a static attendee roster (see ATTENDEE_ROSTER_PATH docstring
    # above). Skipped entirely if this event's config doesn't set one.
    if ATTENDEE_ROSTER_PATH:
        pre_event = build_pre_event_analysis(instance_url, token, ATTENDEE_ROSTER_PATH, ABM_TIER_FIELD)
        if pre_event is not None:
            (OUT_DIR / "pre_event_analysis.json").write_text(json.dumps(pre_event, indent=2))
            print(f"Wrote pre-event analysis ({pre_event['summary']['attending_accounts']} matched "
                  f"accounts, {pre_event['summary']['unmatched_companies']} unmatched) to "
                  f"{OUT_DIR}/pre_event_analysis.json")
        else:
            print("Pre-event analysis skipped (see stderr above) -- non-fatal.", file=sys.stderr)


if __name__ == "__main__":
    main()
