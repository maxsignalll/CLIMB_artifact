from __future__ import annotations

from pathlib import Path


TABLE = r"""\begin{table*}[t]
\centering
\small
\setlength{\tabcolsep}{6pt}
\begin{tabular}{lccccc}
\toprule
\textbf{Policy} & \textbf{HardGate} & \textbf{Deficit fairness} & \textbf{Stability guardrails} & \textbf{Class-level DRR} & \textbf{VIP rescue} \\
\midrule
\texttt{gate\_rr} (CLIMB) & \textbf{Y} & -- & -- & -- & -- \\
\texttt{legacy} (legacy) & \textbf{Y} & \textbf{Y} & \textbf{Y} & -- & -- \\
\texttt{legacy\_no\_gate} & -- & \textbf{Y} & \textbf{Y} & -- & -- \\
\texttt{legacy\_no\_deficit} & \textbf{Y} & -- & \textbf{Y} & -- & -- \\
\texttt{legacy\_no\_stability} & \textbf{Y} & \textbf{Y} & -- & -- & -- \\
\texttt{gate\_rr\_pp} & \textbf{Y} & -- & -- & \textbf{Y} & \textit{opt.} \\
\bottomrule
\end{tabular}
\vspace{2pt}
\caption{Explored policy variants and the additional mechanisms they introduce. ``HardGate'' denotes admission that constrains the active set by K. ``Stability guardrails'' refers to lease/switch-budget/cooldown style knobs. \texttt{gate\_rr\_pp} keeps \texttt{gate\_rr} admission but adds class-level DRR for dispatch and an optional rescue mechanism.}
\label{tab:policy_variants}
\end{table*}
"""


def main() -> None:
    base = Path(__file__).resolve().parents[1]
    out_path = base / "docs" / "figures" / "tab_variant_mechanisms.tex"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(TABLE)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
