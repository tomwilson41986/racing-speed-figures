"""Timeform TFig reconciliation.

Pulls the previous day's Timeform time figures (TFigs) from
``timeform.com/horse-racing/results/yesterday`` (behind login), matches them to
our own computed UK/IRE speed figures for that day, emails a daily comparison
report, and maintains a guarded rolling correction that can optionally be fed
back into the live figures.

Design note: ``timeform.com`` is unreachable from the dev sandbox (egress
policy), so the network layer (``auth``/``client``) only runs in CI, while all
parsing/matching/scoring logic (``results``/``reconcile``/``store``) is written
as pure functions and unit-tested against saved fixtures.
"""
