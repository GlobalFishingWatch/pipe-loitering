from pipe_loitering.utils.ver import get_pipe_ver


def test_get_pipe_ver():
    match = get_pipe_ver()
    assert match
