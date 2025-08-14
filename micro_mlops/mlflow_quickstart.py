import mlflow
import mlflow.sklearn
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
import pandas as pd

# 1) Make a small synthetic classification dataset (fast & reproducible)
X, y = make_classification(
    n_samples=600, n_features=8, n_informative=5, n_redundant=1,
    random_state=42, class_sep=1.2
)

# 2) Train/test split so metrics reflect generalization (not memorization)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

# 3) Choose an MLflow "experiment" (like a project bucket)
mlflow.set_experiment("heart_risk_demo")

# 4) Start a tracked run; everything inside gets recorded
with mlflow.start_run(run_name="rf_baseline_v1"):
    # (a) Hyperparameters: critical for reproducibility and comparisons
    params = {"n_estimators": 200, "max_depth": 6, "random_state": 42}
    mlflow.log_params(params)

    # (b) Train the model
    model = RandomForestClassifier(**params)
    model.fit(X_train, y_train)

    # (c) Evaluate: accuracy (overall) + F1 (balances precision/recall; great for health)
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("f1", f1)

    # (d) Artifact: save predictions for audit/debug (important in regulated settings)
    df_eval = pd.DataFrame({"y_true": y_test, "y_pred": y_pred})
    df_eval.to_csv("eval_predictions.csv", index=False)
    mlflow.log_artifact("eval_predictions.csv")

    from mlflow.models import infer_signature

# Infer signature from training data & predictions
signature = infer_signature(X_train, model.predict(X_train))

# Log the trained model using new 'name=' argument
mlflow.sklearn.log_model(
    sk_model=model,
    name="model",                 # replaces artifact_path (deprecated)
    signature=signature,
    input_example=X_train[:5]     # tiny example for schema docs
)


print("Logged run. Optional UI: `mlflow ui --port 5000` → http://127.0.0.1:5000")
