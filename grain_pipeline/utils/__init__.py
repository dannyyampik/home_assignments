"""Shared infrastructure.

Nothing in this package knows anything about clients, trades or FX rates. It
holds the machinery every model reuses — configuration, logging, SQL fragments,
the counted-filter chain, and the quality-check framework — so that each model
under ``grain_pipeline.pipeline`` contains only the logic specific to the table
it builds.

The split is what keeps the models comparable to one another: two models that
both normalise an identifier call the same fragment rather than each spelling
out ``upper(nullif(trim(...), ''))``, so a change to the canonicalisation rule
happens once and applies everywhere.
"""
