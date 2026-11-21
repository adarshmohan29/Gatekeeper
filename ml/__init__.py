"""Training and evaluation (host only; the services serve exported ONNX models)."""

# Load order matters on macOS: torch and XGBoost each bundle an OpenMP runtime, and if
# torch's loads first, XGBoost segfaults while training. Importing XGBoost here, before
# any module in this package can import torch, fixes the order for every entry point.
import xgboost  # noqa: F401
