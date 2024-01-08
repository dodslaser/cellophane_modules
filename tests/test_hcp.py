from pathlib import Path
from unittest.mock import MagicMock

from cellophane import Sample
from pytest import fixture, mark, param, raises
from pytest_mock import MockerFixture

from cellophane_modules import hcp

INTEGRATION = Path(__file__).parent / "integration"


@fixture(scope="class")
def hcp_sample():
    return Sample.with_mixins([hcp.HCPSample])


class Test__fetch:
    @staticmethod
    def test__fetch(tmp_path, mocker: MockerFixture):
        _hcpm_mock = MagicMock()
        _hcpm_factory_mock = mocker.patch(
            "cellophane_modules.hcp.HCPManager",
            return_value=_hcpm_mock,
        )

        assert (
            hcp._fetch(
                credentials="CREDS",
                local_path=tmp_path / "foo",
                remote_key="foo",
            )[0]
            == "hcp"
        )

        (tmp_path / "bar").touch()

        assert (
            hcp._fetch(
                credentials="CREDS",
                local_path=tmp_path / "bar",
                remote_key="bar",
            )[0]
            == "local"
        )

        _hcpm_factory_mock.assert_called_once_with(
            credentials_path="CREDS",
            bucket="data",
        )

        _hcpm_mock.download_file.assert_called_once_with(
            "foo",
            local_path=str(tmp_path / "foo"),
            callback=False,
            force=True,
        )


class Test_HCPSample:
    @staticmethod
    @mark.parametrize(
        "kwargs,exception",
        [
            param(
                {"hcp_remote_keys": None},
                None,
                id="none",
            ),
            param(
                {"hcp_remote_keys": []},
                None,
                id="empty",
            ),
            param(
                {"hcp_remote_keys": [1337]},
                TypeError,
                id="bad_backup_value_type",
            ),
            param(
                {"hcp_remote_keys": ["foo"], "files": ["boo"]},
                None,
                id="single",
            ),
            param(
                {"hcp_remote_keys": ["foo", "bar"], "files": ["boo", "baz"]},
                None,
                id="multiple",
            ),
            param(
                {"hcp_remote_keys": ["foo", "bar"], "files": ["boo"]},
                ValueError,
                id="length_mismatch",
            ),
        ],
    )
    def test_validate(hcp_sample, kwargs, exception):
        assert (
            raises(exception, hcp_sample, id="DUMMY", **kwargs)
            if exception
            else hcp_sample(id="DUMMY", **kwargs) is not None
        )
