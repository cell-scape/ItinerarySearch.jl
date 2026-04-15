# MCT User Guide Summary

**Source:** IATA Minimum Connecting Time User Guide v1.2 (March 2021)
**Reference:** `docs/reference/minimum-connecting-time-user-guide_version-1.-2.pdf`

This document summarizes the IATA MCT User Guide, which provides guidance for processing MCT filings according to SSIM Chapter 8. The MCT Data Elements were added to the 2018 SSIM Chapter 8.

## Key Concepts

**MCT** is the shortest time interval required to transfer a passenger and luggage from one flight to a connecting flight at a specific location. Governed by IATA PSC Resolution 765, MCTs must be observed by all ticketing/reservation systems.

**MCT time is in HHMM format.** Carriers can file MCTs up to 9959 (99 hours 59 minutes). MCTs above 24 hours are permitted but connections may not be guaranteed to build. Most systems build connections over 24 hours as two separate segments (Example 44).

**Suppression time field must be blank.** Filing 19:59/999 to suppress connections is no longer accepted and will be rejected. The Suppression Indicator (Y/N) must be used instead (Example 42).

## Record Types

1. **Station Standard MCT** — No carrier specified. Published by IATA. Provides default MCT for a station.
2. **MCT Exception** — At least one carrier specified. Filed bilaterally between carriers. Overrides station standards.
3. **MCT Suppression** — Suppression Indicator = Y. Blocks specific connections. Can be station-specific or geographic (global).

## MCT Carrier Submission Template (Section III)

The template has 4 sections of data elements:

**Section 1:** Action Indicator (A/D), Station (Arr/Dept), Connection Status (DD/DI/ID/II), Time (HHMM), Arr Carrier (Carrier, CS Indicator, CS Operating), Arr Flight Range (Start/End)

**Section 2:** Departure Carrier (Carrier, CS Indicator, CS Operating), Dep Flight Range (Start/End), Terminal (Arrive/Depart), Station (Prev/Next), State (Prev/Next)

**Section 3:** Country (Prev/Next), Region (Prev/Next), Aircraft Type (Arrive/Depart), Aircraft Body W/N (Arrive/Depart)

**Section 4:** Suppressions (Suppression Indicator Y/N, Region, Country, State), Date (Effective From/To)

## Connection Building Filter (Section IV)

Carriers can submit a list of their interline and codeshare partners. Only those carriers listed will be considered for connection building, subject to hierarchy rules. Carriers not listed will never build connections. All online connections for the submitting carrier can still build.

## Suppression Geography

**Suppression geography (Region/Country/State) refers to the connection station's geography**, not the origin/destination of the flights. Examples:

- `supp_country = "CU"` — suppress all connections at stations located in Cuba (Example 2, p.16)
- `supp_region = "EUR"` — suppress connections at European stations (Example 25, p.40)
- `supp_state = "NY"` — suppress connections at stations in New York state (Example 21, p.36)

A suppression with all geography fields blank is a **global suppression** — applies at all stations worldwide (Examples 1, 22, 28, 34).

**Important:** A global suppression can be overridden by filing a station-specific exception with Suppression Indicator = N (Example 1: global suppression AA→VY, with BCN/FCO exceptions).

## Codeshare Handling

**Codeshare Indicator (Y)** — MCT applies specifically to codeshare flights (determined by DEI 50).

**Codeshare Operating Carrier** — Specifies which operating carrier the MCT applies to.

**Default to Operating Carrier MCT** — When a marketing carrier files no MCT exceptions at a station, the system defaults to the operating carrier's MCT values based on DEI 50 filings (Examples 12-13). A marketing MCT is not necessary unless the marketing carrier wants a **longer** MCT than the operating carrier.

**Global codeshare suppression** — A carrier can suppress all codeshare connections globally, then file station-specific exceptions to allow connections at specific stations (Example 22: AA suppresses codeshare globally, allows at JFK with MCT 2:00).

**Transition from flight ranges to operating carrier** — Carriers are encouraged to replace codeshare flight number ranges with Codeshare Operating Carrier codes, reducing the number of filings needed while providing improved clarity (Examples 15-16).

## Previous/Next Station, State, Country, Region

These fields specify the origin of the arriving flight (Previous) and destination of the departing flight (Next). They enable MCTs based on routing:

- **Next/Previous Station** — Specific airport code (Example 11: LED→TLL→KBP, MCT 3 with nxt_stn=KBP takes priority)
- **Next/Previous Country** — ISO country code (Example 9: WAW, nxt_country=US takes priority over generic MCT)
- **Next/Previous State** — State/province code (Example 27: MCO, suppress NJ→IL and IL→NJ state pairs)
- **Next/Previous Region** — IATA region code (Example 10: WAW, next_region=SCH takes priority; Example 36: MAN, prev_region=CAR)

**Specificity hierarchy:** More specific geography takes precedence. A record with Next Station Code beats one with only Next Country, which beats one with only Next Region.

## Aircraft Body and Type

- **Aircraft Body (W/N)** — Wide or Narrow body classification. W-body equipment typically requires longer MCT (Example 4: DFW, AA W-body arrival requires 85 min vs 80 min for narrow).
- **Aircraft Type** — IATA 3-character equipment code. MCT can be specified for specific aircraft types (Example 17: FRA, ZZ 380 aircraft requires 1:30).

Body and Aircraft Type are mutually exclusive per the SSIM specification.

## Date Validity

- **Effective From / Effective To** — Date range during which the MCT applies (Example 5: LAX, AA seasonal MCT increase 27-Mar to 26-Oct).
- Records with dates take priority over those without (SSIM Ch. 8 hierarchy priorities 25-26).
- Dates are in local time at the connection airport.

## Flight Number Ranges

- **Arr/Dep Flight Range (Start/End)** — MCT applies only to flights within the specified range.
- A flight-specific MCT uses the same value for Start and End (Example 32: LAX, UA 5210-5210).
- Overlapping flight ranges within the same filing are not allowed and will be rejected (Example 33: IAD, MCT 3 rejected due to overlap with MCT 1 and 2).
- Sub-set flight ranges can have different MCTs than the parent range (Example 33: valid non-overlapping sub-ranges).

## Key Filing Rules

1. **Bilateral agreement** — MCT exceptions between two carriers may require concurrence from the receiving carrier (Section II process flow).
2. **Action indicators** — A (Add) and D (Delete). Changes are a delete+add pair; all fields must match for a delete to be actioned (Example 8).
3. **Data aggregators** — OAG (MCT@oag.com) and Cirium (MCT@Cirium.com) process and distribute MCT data.
4. **Time limit** — MCTs can be filed up to 9959 HHMM. Values above 24:00 are permitted but may not build in all systems (Example 44).

## Global Suppression Best Practices (Example 45)

When filing global suppressions, IATA recommends:
- Use geographic suppressions (Region/Country) rather than blank-station global suppressions when the carrier only operates in specific regions
- File station-specific suppressions rather than global when practical
- This avoids unintended blocking of connections at stations where the carrier doesn't operate

## Examples Index

| # | Topic | Key Concept |
|---|-------|-------------|
| 1 | Suppression - Global Carrier | Global suppression with station overrides |
| 2 | Suppression - Country | supp_country=CU blocks all Cuba stations |
| 3 | Suppression - Global Codeshare | Suppress all CS, allow specific CS carrier-to-carrier |
| 4 | Aircraft Body Override | W-body requires longer MCT |
| 5 | Dates - Effective from/to | Seasonal MCT increase |
| 6 | Codeshare Indicator | CS indicator with SCH region |
| 7 | Operating Carrier MCT | Default to operating carrier values |
| 8 | Applying MCT Changes | Delete + Add pattern |
| 9-11 | Next/Previous Geography | Country, region, station codes |
| 12-13 | Default to Operating Carrier | DEI 50 fallback behavior |
| 14 | Codeshare Flights | Multiple filing options for CS |
| 15-16 | Flight Range → CS Operating | Transition to operating carrier codes |
| 17 | Equipment Exceptions | Aircraft type + body MCTs |
| 18 | Date Range | Seasonal date breaks |
| 19-21 | Geography - All Carriers | Country/region suppression, circuitous routing |
| 22 | Global CS Suppression + Exception | Suppress globally, allow at specific stations |
| 23 | Time Exception | Basic time filing |
| 24 | Basic Suppression | Y indicator vs 999 time |
| 25-27 | Suppression by Region/Country/State | Geographic suppression scoping |
| 28 | Global Scale Suppression | All-carrier CS suppression |
| 29-31 | Station + Geography | Station-country, station-region combinations |
| 32 | Flight Specific MCT | Single flight number range |
| 33 | Flight Range + Sub-set | No overlapping ranges |
| 34 | Global Suppression - CS Only | Suppress all except partner carriers |
| 35-36 | Station - State/Region | State and region-level exceptions |
| 37-38 | All-Carrier Suppression | Suppress including self |
| 39-41 | All-Carrier Suppression by Geo | Region/country/state scoping |
| 42 | Time Exception Filing | 999/19:59 no longer valid |
| 43 | Connection Building Filter | Interline partner list |
| 44 | Time Limit | MCT up to 9959 HHMM |
| 45 | Global Suppression Recommendations | Use geographic scoping |
