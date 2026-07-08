"""Unit tests for France pipeline constants and unit-scale helpers.

These pin down two historical unit bugs:
  1. The GA prior fallback was a bare 0.05 — a seconds-per-FURLONG value
     used on the seconds-per-METRE scale (~100x too large).
  2. The beaten-length attenuation threshold applied the UK's -8
     lengths-per-(s/f) sensitivity directly to s/m GAs, making the going
     dependence numerically inert.
"""

import numpy as np
import pytest

from src.france import constants as C


M = C.METERS_PER_FURLONG


class TestGoingPriorLookup:
    def test_exact_match(self):
        assert C.get_france_going_prior("Bon") == pytest.approx(0.118 / M)
        assert C.get_france_going_prior("Lourd") == pytest.approx(0.705 / M)

    def test_exact_match_preserves_casing_variants(self):
        # Casing variants deliberately carry different empirical values.
        assert C.get_france_going_prior("Très souple") == pytest.approx(0.569 / M)
        assert C.get_france_going_prior("Tres souple") == pytest.approx(0.52 / M)

    def test_case_and_accent_insensitive_fallback(self):
        # Unseen upper-cased/accent-stripped variants resolve via the
        # normalised map instead of the (previously absurd) default.
        assert C.get_france_going_prior("TRÈS SOUPLE") == pytest.approx(0.569 / M)
        assert C.get_france_going_prior("COLLANT ") == pytest.approx(0.919 / M)
        assert C.get_france_going_prior("bon  léger") == pytest.approx(0.195 / M)

    def test_unknown_uses_metre_scale_default(self):
        val = C.get_france_going_prior("Going Nobody Has Seen")
        assert val == pytest.approx(C.DEFAULT_GA_PRIOR)
        # The default MUST be on the s/m scale (0.05 s/f ÷ 201.168),
        # not a bare 0.05 (which is ~10 s/f).
        assert val == pytest.approx(0.05 / M)
        assert val < 0.001

    def test_none_and_nan_inputs(self):
        # None normalises to "" which has its own empirical prior.
        assert C.get_france_going_prior(None) == pytest.approx(0.133 / M)
        assert C.get_france_going_prior(float("nan")) == pytest.approx(
            C.DEFAULT_GA_PRIOR
        )

    def test_all_priors_are_plausible_sm_magnitudes(self):
        # Nothing in the table should exceed ~4 s/f once converted.
        for going, val in C.FRANCE_GOING_GA_PRIOR.items():
            assert abs(val) < 4.0 / M, f"{going!r} = {val} is not on s/m scale"


class TestBlAttenuationThreshold:
    def test_neutral_going_keeps_base_threshold(self):
        assert C.bl_attenuation_threshold(0.0) == pytest.approx(20.0)

    def test_soft_going_lowers_threshold(self):
        # GA of +1.0 s/f expressed in s/m must shift T by -8 lengths.
        ga_sm = 1.0 / M
        assert C.bl_attenuation_threshold(ga_sm) == pytest.approx(12.0)

    def test_firm_going_raises_threshold(self):
        ga_sm = -0.5 / M
        assert C.bl_attenuation_threshold(ga_sm) == pytest.approx(24.0)

    def test_clipped_to_bounds(self):
        assert C.bl_attenuation_threshold(3.0 / M) == pytest.approx(10.0)
        assert C.bl_attenuation_threshold(-3.0 / M) == pytest.approx(30.0)

    def test_not_inert_for_realistic_sm_values(self):
        # The original port applied -8 directly to s/m GAs, leaving the
        # threshold at ~20 for every real meeting.  A typical soft-ground
        # GA (~0.5 s/f = 0.0025 s/m) must move the threshold materially.
        t = C.bl_attenuation_threshold(0.5 / M)
        assert t == pytest.approx(16.0)

    def test_vectorised_input(self):
        ga = np.array([0.0, 1.0 / M, -0.5 / M])
        out = C.bl_attenuation_threshold(ga)
        assert np.allclose(out, [20.0, 12.0, 24.0])
