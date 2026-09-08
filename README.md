# PyCJr

Python driver and tooling for the IBM PCjr (4860/4861) infrared keyboard
link. Sends keystrokes over a 40 kHz IR carrier via pigpio on GPIO18 and
serves the byte-pipeline MCP for 8088 assembly build/disassemble/lint.

- `pycjr.py` — Pi-side IR sender
- `mcp/` — pcjr-tools MCP server
- `bin/` — build, lint, selftest, server-start scripts

The hardware research ledger lives in
[pcjr-ir-lab](https://github.com/AxeZ0N/pcjr-ir-lab) — facts, session
handoffs, and ground-truth anchors stay there, not here.
