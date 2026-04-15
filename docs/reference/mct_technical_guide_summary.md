# MCT Technical Guide Summary

**Source:** IATA Minimum Connecting Time Technical Guide v2.2 (March 2021)
**Reference:** `docs/reference/minimum-connecting-time-technical-guide_version-2.2.pdf`

This document is aimed at development teams implementing the SSIM Chapter 8 MCT hierarchy. It provides worked examples with flight segments, MCT tables, and expected build/no-build outcomes. It also contains an authoritative FAQ section with clarifications from IATA.

## Working with Flight Ranges

- **Next Country takes priority** over a generic flight range MCT (Example 1: WAW, MCT 1 with nxt_country=US overrides MCT 2 without)
- **Overlapping flight ranges are not allowed.** An overlap occurs when either the arrival or departure flight range overlaps AND there is a subset flight range on the opposite side (Example 2: LHR, arr ranges 1000-2000 and 1500-2500 overlap — rejected)

## Working with Dates

- **Records with dates take priority** over records without dates (SSIM hierarchy priorities 25-26)
- **Duplicate detection rules:**
  - **Rule 1:** Records with identical times and all same criteria except dates are only considered duplicates if the hierarchy cannot resolve them (i.e. all other fields match)
  - **Rule 2:** If the hierarchy cannot decide, any overlap or subset of Effective From/To should be rejected as a duplicate
- **12 date overlap examples** (p.5): blank+blank, blank+date, date+blank, date+date combinations. Non-overlapping date ranges are allowed (Example 7). Any overlap on even one day is rejected (Examples 8-10). Totally overlapping (one included in the other) is rejected (Examples 9, 11, 12).

## Working with Codeshare

**Key principle:** For codeshare flights, MCTs are established by the operating carrier unless overwritten by the marketing carrier. If no MCT matching the marketing flight exists, the operating flight's MCT (identified via DEI 50) is used.

- **Example 1 (MIA):** Marketing carrier AA files MCT 1 (145 min, cs_ind=Y, cs_op=BA) for codeshare AA→AA. Operating carrier BA has MCT 2 (130 min, BA→AA). AA6200 (DEI50=BA0207) connecting to AA2213: MCT 1 applies (145 min, marketing overrides operating). BA0207→AA2213: MCT 2 applies (130 min, not a codeshare).

- **Example 2 (FRA):** LO codeshare with LH. MCT 1 (45 min, LO cs_ind=Y cs_op=LH → LH) applies to codeshare flights. MCT 2 (100 min, LO → blank, prev_country=PL, next_region=SCH) applies to LO operating flights from Poland to Schengen. LO379→LH1082: MCT 2 applies (operating LO, from PL to SCH). LO379→LO4733 (DEI50=LH1082): MCT 2 also applies. LO379→LH1084/LO4741 (DEI50=LH1084): MCT 2 applies.

- **Example 3 (SVO):** DL codeshares with AF and SU. No DL MCT exceptions at SVO. Based on DEI 50, MCT defaults to operating carrier values. DL8614 (DEI50=AF1200) arriving SVO Term E, connecting to DL9096 (DEI50=SU1200) departing Term D: uses MCT 2 (130 min, SU→SU, Term E→D).

- **Example 4 (AMS):** DL codeshares with KL. MCT 1 (50 min, KL→KL) is the baseline. MCT 2 (130 min, KL→KL, flt 210-210) is flight-specific. MCT 3 (50 min, KL→KL, flt 400-1999) is a range. MCT 4 (50 min, station default). DL9667 (DEI50=KL0624) → DL9556 (DEI50=KL1765): uses MCT 1 (50 min). DL0072 → DL9556: uses MCT 4 (station default, no applicable exception).

- **Example 5 (JFK):** DL codeshares with VS. Five MCT levels demonstrate the hierarchy: MCT 1 (DL→DL), MCT 2 (DL→DL flt 4339-4438), MCT 3 (DL→DL cs_ind=Y), MCT 4 (DL→DL cs_ind=Y cs_op=VS), MCT 5 (DL→DL cs_ind=Y cs_op=VS flt 4339-4438). **MCT 1 and 2 apply to operating flights only.** MCT 3-5 are all valid for codeshare. **MCT 5 is selected based on hierarchy** (most specific: codeshare + operating carrier + flight range).

## Working with Suppressions

- **Example 1:** Global suppression AA→VY (blank station, supp_ind=Y) with station overrides at BCN/FCO (supp_ind=N with time). AA0520→VY2134 at MAD: suppressed by MCT 1 (global). AA0620→VY3125 at BCN: builds with MCT 3 (station override). BA0700→AA6100 (DEI50=BA0700) at MAD: builds with MCT 5 (not codeshare, different carriers).

- **Example 2:** Country-level suppression. AA files supp_country=CU for HAV. AA0801→CU0826 at HAV: suppressed. CU0827→AA0802 at HAV: suppressed (both directions covered).

- **Example 3:** Global codeshare suppression AA→AA (cs_ind=Y, supp_ind=Y) with station-specific override at AMS (cs_ind=Y, cs_op=BA→LY, supp_ind=N). BA0440→LY0336 at AMS: uses MCT 2 (operating carriers, not codeshare). AA6100→AA8100 (both codeshare): uses MCT 3 (codeshare exception at AMS). BA0902→CX0288 at FRA: uses MCT 4 (different carriers, not affected by AA suppression). AA6102→AA7200 at FRA: suppressed by MCT 1 (codeshare, no FRA override).

- **Example 4:** Global codeshare suppression AA→AA (cs_ind=Y). IB3172→BA0257 at LHR: builds with MCT 2 (IB→BA, not codeshare). AA8102→AA6150 at LHR: suppressed by MCT 1 (both are codeshare flights of the same connection).

## Working with the Connection Building Filter (Effective 01NOV22)

**Record Type 3** — a new MCT record type for partnership lists.

- **Example 1:** AA files partnership list [BA, LH]. Only AA-BA, BA*AA-BA, AA-LH, LH*AA-BA connections build. All AA online connections build. AA-VY does not build (VY not in list).

- **Example 2:** AA [BA, LH], BA [AA, VY], LH [UA] all file lists. AA-LH does NOT build (LH doesn't list AA). BA-LH does NOT build (neither lists the other). BA*AA-VY*BA does NOT build (operating connection is BA-VY which works, but marketing carriers AA and VY — AA doesn't list VY).

- **Example 3:** Multi-segment itinerary JFK(AA)-LHR(AA*BA)-MAD(VY)-BCN. Each connection checked separately. LHR connection builds (AA and BA are in each other's lists). MAD connection builds (BA has VY; VY has no list so all carriers allowed). But JFK(AA)-LHR(AA*BA)-MAD(VY)-BCN where AA connects to VY at MAD would NOT build (AA doesn't list VY).

**Key rules:**
- The filter **supersedes all MCT records** — if a carrier is not in the partnership list, no MCT is applied, the connection simply doesn't build
- The filter is checked **per connection**, not per itinerary — other connections in the same itinerary are unaffected
- **Both marketing and operating carriers** (via DEI 50) must be in the respective partnership lists
- Carriers not filing a partnership list allow connections with all carriers
- All online connections for the submitting carrier are always allowed

## Working with Other Data Elements

- **Aircraft Body W arrival** requires longer MCT (Example 1: DFW, AA MCT 1 = 120 min for AA→AA, MCT 2 = 125 min for AA→AA with arr_body=W). AA0280 on 789 (W-body) uses MCT 2 (125 min). AA1387 on 321 (N-body) uses MCT 1 (120 min). Codeshare MH9428 (DEI50=AA0280) also uses MCT 2 via operating carrier default.

## FAQ Highlights

### General
- **Global defaults** if no station standard: DD 00:30, DI 01:00, ID 01:30, II 01:30
- **Station standard** = no Arrival Carrier, no Arrival Operating Carrier, no Departure Carrier, no Departure Operating Carrier. Published by IATA/data aggregators.
- **DD/DI/ID/II status is mandatory** for all records including suppressions
- Global defaults can optionally be delivered in the MCT file (not required to be hard-coded)
- **Terminal codes:** left-justified, blank-filled (1 or 2 characters)
- **Date format:** official IATA is 7 chars uppercase `DDMMMYY` (e.g. "03MAR18", "26OCT17")
- **Dates are local time** at the connect airport. Geographic suppressions also use local time.

### Hierarchy
- **File order is irrelevant** — do not rely on record order in the MCT file
- **Identical records with different times shall not happen** — data aggregators ensure this

### Flight Legs vs Segments
- **International/domestic status** may be applied at leg or segment level depending on the system. DEI 220 specifies how it should be applied and overrides any default interpretation.

### Flight Number Ranges
- **Both start and end must be set** (two hierarchy levels for future flexibility)
- **Subset ranges take priority** over the larger range. A subset must be completely contained within the parent range (SSIM Ch. 8.9.11).

### Codeshare Indicator
- **For codeshare flights, MCTs are established by the operating carrier**, unless overwritten by the marketing carrier
- The MCT for a marketing flight is found by referring to DEI 50 on the published flight
- **If no MCT matching the marketing flight exists, the operating flight is considered relevant** — no MCTs need to be filed for marketing flights when the operating carrier's MCT should apply

### Aircraft (Body) Types
- **If no body type exists (e.g. TRN), use the aircraft type instead**
- **Only W and N are valid** body type values (or blank)
- **Aircraft type matches exact type** only — filing "737" does NOT match "73H", "738", etc. Each specific type must be filed separately.
- **Aircraft Type and Body are mutually exclusive** — aggregators should reject records with both

### Regions
- **All IATA regions are available** (not just EUR and SCH). See SSIM Appendix I, Chapter 2.1.
- **SCH takes priority over EUR.** Think of SCH as a subset of EUR.
- Region definitions come from SSIM Appendix I (no separate definition file).

### Suppressions
- **A record is a suppression if and only if Suppression Indicator = "Y"** (True)
- **Valid values: Y or N only.** Blanks are converted to N by aggregators.
- **Effective From/To dates are allowed** for suppressions
- **DD/DI/ID/II is mandatory** for suppressions
- **A suppression is global** when ALL of: arrival station, departure station, suppression region, suppression country, and suppression state are empty
- **Arrival and Departure station CAN be filed** for suppressions (they are not required to be blank)
- **Minimum valid suppression elements:** Action Indicator, Status, Arrival Carrier and/or Departure Carrier, Suppression Indicator

### Connection Building Filter
- **Supersedes all MCT records** — connection with an unlisted carrier simply doesn't build, regardless of MCTs
- **Per-connection basis** — each connection within an itinerary is checked separately against the partnership lists
- The filter does not prevent other connections in the itinerary from building with carriers the filter carrier doesn't participate in
- **Both marketing AND operating carriers** must be in the partnership lists

### File Layout
- **Only 2-character IATA airline codes** — ICAO 3-letter codes are not allowed in MCT filings

## Implementation Notes

Key differences/clarifications from this guide vs our current implementation:

1. **Global defaults** — DD=30, DI=60, ID=90, II=90 (fixed in code to match SSIM Ch. 8 and IATA FAQ)
2. **Subset flight ranges take priority** over parent ranges — our specificity calculation should account for this
3. **Aircraft type is exact match only** — "737" ≠ "73H"/"738" etc.
4. **File order is irrelevant** — we should not rely on record serial for hierarchy resolution (only as tiebreaker when specificity is equal)
5. **Connection Building Filter** (Record Type 3) is not yet implemented — this is a separate pre-MCT check that supersedes all MCT matching
