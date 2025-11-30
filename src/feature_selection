import argparse
import json
import time
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import VarianceThreshold
from sklearn.model_selection import train_test_split


def train_eval_rf(X, y, random_state=42):
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=random_state, stratify=y
    )
    clf = RandomForestClassifier(
        n_estimators=200,
        random_state=random_state,
        n_jobs=-1,
        class_weight="balanced",
    )
    clf.fit(X_train, y_train)
    return clf, clf.score(X_val, y_val)


def baseline_selection(df: pd.DataFrame):
    feature_cols = [c for c in df.columns if c not in ["image_id", "label"]]
    X = df[feature_cols].values
    y = df["label"].values

    selector = VarianceThreshold(threshold=0.0)
    X_sel = selector.fit_transform(X)

    selected_idx = np.where(selector.get_support())[0]
    selected_features = [feature_cols[i] for i in selected_idx]

    _, acc = train_eval_rf(X_sel, y)

    metrics = {
        "baseline_accuracy": float(acc),
        "baseline_num_features": int(len(selected_features)),
    }

    return selected_features, metrics


def ga_feature_selection(df: pd.DataFrame,
                         pop_size: int = 20,
                         n_generations: int = 10,
                         penalty: float = 0.001,
                         random_state: int = 42):
    rng = np.random.RandomState(random_state)

    feature_cols = [c for c in df.columns if c not in ["image_id", "label"]]
    X = df[feature_cols].values
    y = df["label"].values
    n_features = X.shape[1]

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=random_state, stratify=y
    )

    def evaluate(mask: np.ndarray) -> float:
        if mask.sum() == 0:
            return 0.0
        X_tr = X_train[:, mask]
        X_v = X_val[:, mask]
        clf = RandomForestClassifier(
            n_estimators=200,
            random_state=random_state,
            n_jobs=-1,
            class_weight="balanced",
        )
        clf.fit(X_tr, y_train)
        acc = clf.score(X_v, y_val)
        acc_penalized = acc - penalty * mask.sum()
        return acc_penalized

    pop = []
    for _ in range(pop_size):
        m = rng.rand(n_features) < 0.5
        if not m.any():
            m[rng.randint(0, n_features)] = True
        pop.append(m)

    def tournament_selection(pop, fitnesses, k=3):
        idxs = rng.choice(len(pop), size=k, replace=False)
        best = idxs[0]
        best_fit = fitnesses[best]
        for i in idxs[1:]:
            if fitnesses[i] > best_fit:
                best = i
                best_fit = fitnesses[i]
        return pop[best].copy()

    def crossover(p1, p2):
        point = rng.randint(1, n_features - 1)
        c1 = np.concatenate([p1[:point], p2[point:]])
        c2 = np.concatenate([p2[:point], p1[point:]])
        return c1, c2

    def mutate(m, prob=0.05):
        flip = rng.rand(n_features) < prob
        m[flip] = ~m[flip]
        if not m.any():
            m[rng.randint(0, n_features)] = True
        return m

    t0 = time.time()
    fitnesses = np.array([evaluate(ind) for ind in pop])

    for gen in range(n_generations):
        new_pop = []
        while len(new_pop) < pop_size:
            p1 = tournament_selection(pop, fitnesses)
            p2 = tournament_selection(pop, fitnesses)
            c1, c2 = crossover(p1, p2)
            c1 = mutate(c1, prob=1.0 / n_features)
            c2 = mutate(c2, prob=1.0 / n_features)
            new_pop.extend([c1, c2])
        pop = new_pop[:pop_size]
        fitnesses = np.array([evaluate(ind) for ind in pop])
        print(f"[GA] Generation {gen+1}/{n_generations} best fitness={fitnesses.max():.4f}")

    best_idx = int(np.argmax(fitnesses))
    best_mask = pop[best_idx]
    best_fitness = float(fitnesses[best_idx])
    best_num_features = int(best_mask.sum())

    selected_features = [feature_cols[i] for i in range(n_features) if best_mask[i]]

    runtime = time.time() - t0
    ga_accuracy_approx = best_fitness + penalty * best_num_features

    metrics = {
        "ga_fitness": best_fitness,
        "ga_accuracy": ga_accuracy_approx,
        "ga_num_features": best_num_features,
        "ga_runtime_seconds": runtime,
    }

    return selected_features, metrics


def main(train_path, selected_out, baseline_out, ga_out):
    print(f"[B] Loading train data from {train_path}")
    df = pd.read_parquet(train_path)

    print("[B] Running baseline feature selection...")
    baseline_feats, baseline_metrics = baseline_selection(df)
    print("[B] Baseline metrics:", baseline_metrics)

    print("[B] Running GA feature selection...")
    ga_feats, ga_metrics = ga_feature_selection(df)
    print("[B] GA metrics:", ga_metrics)

    with open(selected_out, "w") as f:
        json.dump({"selected_features": ga_feats}, f, indent=2)

    with open(baseline_out, "w") as f:
        json.dump(baseline_metrics, f, indent=2)

    with open(ga_out, "w") as f:
        json.dump(ga_metrics, f, indent=2)

    print(f"[B] Saved selected_features.json, baseline_metrics.json, ga_metrics.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--train_path", type=str, required=True)
    parser.add_argument("--selected_features_output", type=str, required=True)
    parser.add_argument("--baseline_metrics_output", type=str, required=True)
    parser.add_argument("--ga_metrics_output", type=str, required=True)
    args = parser.parse_args()

    main(
        args.train_path,
        args.selected_features_output,
        args.baseline_metrics_output,
        args.ga_metrics_output,
    )
