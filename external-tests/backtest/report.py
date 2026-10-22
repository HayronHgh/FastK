"""Render measured application costs as Markdown and native SVG, without images."""

import argparse
import html
import json
from pathlib import Path

LABELS = {"rsi": "RSI(14) 30／70", "sma": "SMA 20／60", "momentum": "60 根動能"}


def render(output):
    manifest = json.loads((output / "manifest.json").read_text())
    summary = json.loads((output / "summary.json").read_text())
    lines = [
        "# 固定回測 × 不同資料儲存方式",
        "",
        "同一份策略、撮合與 NumPy 輸入契約；只有資料後端不同。",
        "",
        f"正式 {manifest['rounds']} 輪、{summary['processes']} 個獨立程序；所有交易、資金曲線、費用與最終持倉一致。",
        "",
        "表格為程序中位數，單位 ms；載入含開啟、讀取、關閉。預載記憶體組不含資料預載成本。",
        "",
    ]
    sections = []
    for mode in manifest["modes"]:
        for case in manifest["cases"]:
            rows = [r for r in summary["results"] if r["mode"] == mode and r["case"] == case]
            title = f"{'逐月' if mode == 'monthly' else '整年一次'} · {LABELS[case]}"
            lines += [
                f"## {title}",
                "",
                "| 後端 | 總時間 | 載入 | 運算 | 載入佔比 | RSS MiB | Private MiB |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
            selected = []
            for r in rows:
                p = r["phase_medians_ns"]
                lines.append(
                    f"| {r['backend']} | {r['total_ns'] / 1e6:.2f} | {p['load_ns'] / 1e6:.2f} | {p['compute_ns'] / 1e6:.2f} | {r['load_fraction'] * 100:.2f}% | {r['rss_bytes'] / 2**20:.2f} | {r['private_bytes'] / 2**20:.2f} |"
                )
                runs = [
                    json.loads((output / j["output"]).read_text())
                    for j in manifest["jobs"]
                    if j["mode"] == mode and j["case"] == case and j["backend"] == r["backend"]
                ]
                selected.append(sorted(runs, key=lambda v: v["total_ns"])[len(runs) // 2])
            lines.append("")
            maximum = max(r["total_ns"] for r in selected)
            svg = [
                '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 960 320" role="img" aria-label="讀取與其餘回測時間">'
            ]
            for i, r in enumerate(selected):
                y = 20 + i * 43
                load = sum(s["load_ns"] for s in r["samples"])
                a, b = load / maximum * 640, (r["total_ns"] - load) / maximum * 640
                svg += [
                    f'<text x="8" y="{y + 18}">{html.escape(r["backend"])}</text>',
                    f'<rect x="100" y="{y}" width="{a}" height="25" fill="#007f86"/>',
                    f'<rect x="{100 + a}" y="{y}" width="{b}" height="25" fill="#666a86"/>',
                    f'<text x="{110 + a + b}" y="{y + 18}">{r["total_ns"] / 1e6:.2f} ms</text>',
                ]
            svg += [
                '<text x="100" y="305">青色：開／讀／關　灰紫色：指標、撮合、曲線、摘要與其他流程</text>',
                "</svg>",
            ]
            sections.append(f"<section><h2>{html.escape(title)}</h2>" + "".join(svg) + "</section>")
    lines += [
        "## 相同的回測結果",
        "",
        "| 策略 | 期末資金 | 報酬率 | 最大回撤 | 成交次數 | 手續費 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for case in manifest["cases"]:
        r = next(r["outcome"] for r in summary["results"] if r["case"] == case)
        equity = (
            f"{r['final_equity']:.2f}" if r["final_equity"] >= 0.01 else f"{r['final_equity']:.6g}"
        )
        lines.append(
            f"| {LABELS[case]} | {equity} | {r['return_fraction'] * 100:.4f}% | {r['max_drawdown'] * 100:.4f}% | {r['fills']} | {r['fees']:.2f} |"
        )
    lines += [
        "",
        "收益只用於驗證後端等價；模型與交易成本見 backtest/README.md，並非策略盈利證據。",
        "",
        "OS cache 未受控，不能稱為 cold-disk；程序啟動、套件匯入與來源預驗證不計入應用時間。",
        "RSS／private 為每 5 ms 取樣的程序峰值中位數，包含匯入；不是結果 buffer 大小。",
        "各階段中位數不必等於總時間中位數。原始五輪樣本、設定、日誌與完整摘要均保留。",
        "",
    ]
    (output / "TABLES.md").write_text("\n".join(lines), encoding="utf-8")
    page = (
        '<!doctype html><html lang="zh-Hant"><meta charset="utf-8"><title>FastK 固定回測</title><style>body{max-width:1000px;margin:40px auto;padding:0 20px;font:16px/1.6 system-ui;color:#222}svg{width:100%;font:15px system-ui}section{border-top:1px solid #ddd;margin-top:36px}h1{font-size:30px}</style><h1>固定回測：儲存對總時間的貢獻</h1><p>2024 BTC／ETH，一分鐘 K 線。相同回測，只換儲存後端。</p><p>圖表各採該設定總耗時居中的實際程序，分段精確加總；統計表另列五程序中位數。memory 是預載資料對照，不是持久化儲存後端。</p>'
        + "".join(sections)
        + "</html>"
    )
    page = page.replace(
        "2024 BTC／ETH，一分鐘 K 線。",
        html.escape("、".join(manifest["common"]["symbols"])) + "。",
    )
    (output / "report.html").write_text(page, encoding="utf-8")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("output", type=Path)
    render(p.parse_args().output)
