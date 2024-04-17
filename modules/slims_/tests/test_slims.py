from pathlib import Path

import slims_
from cellophane.src.testing import parametrize_from_yaml
from pytest import mark, param, raises
from ruamel.yaml import YAML

ROOT = Path(__file__).parent

class Test_integration:
    @staticmethod
    @parametrize_from_yaml([ROOT / "integration.yaml"])
    def test_integration(definition: Path, run_definition):
        run_definition(definition)


class Test_criteria:
    @staticmethod
    @mark.parametrize(
        "criteria,slims,exception,kwargs",
        [
            param(
                d["criteria"],
                d.get("slims"),
                d.get("exception"),
                d.get("kwargs", {}),
                id=d["id"],
            )
            for d in YAML(typ="unsafe").load_all((ROOT / "criteria.yaml").read_text())
        ],
    )
    def test_criteria(criteria, slims, exception, kwargs):
        if exception:
            with raises(exception):
                slims_.parse_criteria(criteria, **kwargs)
        else:
            parsed = slims_.parse_criteria(criteria, **kwargs)
            assert [c.to_dict() for c in parsed] == slims
