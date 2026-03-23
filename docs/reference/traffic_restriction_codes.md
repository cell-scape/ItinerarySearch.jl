# Traffic Restriction Codes (SSIM Appendix G)

Traffic restrictions apply on a segment basis. They control which segments can be displayed and used in connections. When a restriction applies at a Metropolitan Area, it applies to all airports in that area.

Default: In the absence of information to the contrary, any Traffic Restriction stated applies to all forms of traffic (passenger, cargo, mail) at Board and/or Off Point.

## No-Display Codes (no local or connecting traffic)

| Code | Meaning | Display | Connections |
|------|---------|---------|-------------|
| **A** | No Local Traffic | No display | Not allowed |
| **H** | Segment Not To Be Displayed | No display | Not allowed |
| **I** | Technical Landing (non-commercial) | No display | Not allowed |

## Connecting-Only Codes (no local traffic)

| Code | Meaning | Display | Connections |
|------|---------|---------|-------------|
| **K** | Connecting Traffic Only | No display | Allowed |
| **N** | International Connecting Traffic Only | No display | International only |
| **O** | International Online Connecting Traffic Only | No display | International Online only |
| **Y** | Online Connecting Traffic Only | No display | Online only |

## Local-Only Code

| Code | Meaning | Display | Connections |
|------|---------|---------|-------------|
| **B** | Local Traffic Only | Normal display | Not allowed |

## Restricted Connecting Codes

| Code | Meaning | Display | Connections |
|------|---------|---------|-------------|
| **C** | Local and Domestic Connecting Traffic Only | Normal display | Domestic only |
| **F** | Local and Online Connecting Traffic Only | Normal display | Online only |

## Qualified Codes (D/E/G family — trip-level validation)

These codes add an additional constraint: the trip is **invalid** if the D, E, or G restriction exists into AND out of ALL online connect points for the filing carrier(s).

| Code | Meaning | Display | Connections |
|------|---------|---------|-------------|
| **D** | Qualified International Online Connecting or Stopover Traffic Only | Displayed with text | International Online (with qualifier) |
| **E** | Qualified Online Connecting or Stopover Traffic Only | Displayed with text | Online (with qualifier) |
| **G** | Qualified Online Connecting Traffic Only | No display | Online (with qualifier) |

## Stopover Codes

| Code | Meaning | Display (Pax) | Display (Cargo) | Connections |
|------|---------|---------------|-----------------|-------------|
| **M** | International Online Stopover Traffic Only | Displayed with text | No display | Not allowed (Cargo: = Code A) |
| **Q** | International Online Connecting or Stopover Traffic Only | Displayed with text | No display | International Online (Cargo: = Code O) |
| **T** | Online Stopover Traffic Only | Displayed with text | No display | Not allowed (Cargo: = Code A) |

## Combined Connecting/Stopover Codes

| Code | Meaning | Display (Pax) | Display (Cargo) | Connections (Pax) | Connections (Cargo) |
|------|---------|---------------|-----------------|-------------------|---------------------|
| **V** | Connecting or Stopover Traffic Only | Displayed with text | No display | Allowed | = Code K |
| **W** | International Connecting or Stopover Traffic Only | Displayed with text | No display | International | = Code N |
| **X** | Online Connecting or Stopover Traffic Only | Displayed with text | No display | Online | = Code Y |

## Multi-restriction Code

| Code | Meaning | Notes |
|------|---------|-------|
| **Z** | Multiple/Mixed restrictions | Refer to DEI 170-173 for specifics |

## TRC Qualifiers (DEI 710-712)

DEI 710-712 (SSIM Appendix H, pp. 504-506) add board/off point specificity to a TRC:

| DEI | Meaning | Effect |
|-----|---------|--------|
| **710** | TRC applies at **Board Point** only | Blocks connections departing from this point; allows connections arriving here |
| **711** | TRC applies at **Off Point** only | Blocks connections arriving at this point; allows connections departing from here |
| **712** | TRC applies at **both Board and Off Points** | Same as default behavior (no qualifier) |

**Default behavior** (no DEI qualifier): The TRC applies to all traffic types at both board and off points.

These qualifiers matter for multi-leg segments where a single flight has different TRC rules at different intermediate stops. A segment might block connections *from* it (board point restricted) but allow connections *into* it, or vice versa.

**Note:** Multiple TRC qualifiers can exist on the same flight for different carriers. The DEI record's board_point and off_point fields identify which leg segment the qualifier applies to.

## Connection Construction Summary

For passenger itinerary building, the key question is: **Can this segment be used in a connection?**

| Behavior | Codes |
|----------|-------|
| **No connections at all** | A, B, H, I, M, T |
| **Any connection allowed** | K, V |
| **Domestic connections only** | C |
| **International connections only** | N, W |
| **Online connections only** | E, F, G, X, Y |
| **International Online connections only** | D, O, Q |
| **Qualified (D/E/G trip validation)** | D, E, G |

### "Online" definition
Flight Designators of both flights in a connection must use the **same Airline Designator**.

### "International" definition
The connecting segment must be from/to a station in **another country**.

---

## Verification Notes

**Last verified:** 2026-03-17 against IATA SSIM March 2013 Appendix G (pp. 457-459), Appendix H (pp. 501-506), and SSIM 2021 Chapter 8. No TRC changes between 2013 and 2021 editions. The Connection-Building Filter (MCT Record Type 3, effective 01NOV22) is a separate mechanism from TRC and does not modify TRC behavior.

**Corrections applied:**
- Code E added to "Online connections only" summary row (was previously omitted)
- DEI 710-712 (TRC Board/Off Point Qualifiers) section added from Appendix H
