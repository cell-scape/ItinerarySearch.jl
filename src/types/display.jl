# src/types/display.jl — Display style dispatch for audit tooling
#
# Provides:
#   - DisplayStyle     — abstract type for display rendering
#   - PlainStyle       — plain text output (always available)
#
# Extensions (e.g. TermExt) define concrete subtypes with styled rendering.

"""
    abstract type DisplayStyle

Abstract type for MCT inspector display rendering. The base package provides
`PlainStyle` for plain text output. Load Term.jl to get `TermStyle` with
colored panels and tables via the TermExt extension.
"""
abstract type DisplayStyle end

"""
    struct PlainStyle <: DisplayStyle

Plain text display style. All expanded details (leg info, MCT record decode,
codeshare resolution table) are shown as formatted text without colors or
box-drawing characters.
"""
struct PlainStyle <: DisplayStyle end

# Default style — set to TermStyle by TermExt.__init__ when Term.jl is loaded
const _DEFAULT_STYLE = Ref{DisplayStyle}(PlainStyle())
_default_style()::DisplayStyle = _DEFAULT_STYLE[]
