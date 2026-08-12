"""The models.

One module per process — per dimension, per fact, per intermediate lookup. Each
owns everything needed to produce its table: reading its own sources, staging
them, building the output, and asserting its own correctness. A model is the
unit of change; adding one means adding a file and a line in ``run.py``.

    dim_clients           Type 2 SCD over client segmentation
    fx_to_usd             intermediate: base -> USD rate per (currency, date)
    fact_daily_exposure   daily FX exposure at the declared grain

Shared machinery — configuration, logging, SQL fragments, the counted-filter
chain, the quality framework — lives in ``grain_pipeline.utils`` so that these
modules contain only the logic specific to the table they build.

Dependencies between models are ordinary warehouse dependencies and are declared
in each module's docstring: ``fact_daily_exposure`` reads ``stg_clients`` and
``dim_clients`` from the dimension, and ``fx_to_usd`` from the lookup. ``run.py``
resolves the order.
"""
