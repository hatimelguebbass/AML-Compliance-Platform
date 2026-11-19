"""Batch ingestion of the landed source files into the bronze layer.

Runs inside the Spark container, whose interpreter is Python 3.10: modules in this
package and the ones they import must stay compatible with it (enforced by ruff).
"""
