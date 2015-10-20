from unittest import mock

import pytest

@pytest.fixture
def follow_the_leader(request):
    from ..deadman import App
    app = mock.Mock(spec_set=App)
    app.my_id = '42'
    from ..replication import FollowTheLeader
    plugin = FollowTheLeader('zgres#follow-the-leader', app)
    return plugin

def test_no_conn_info(follow_the_leader):
    # can't follow anything without connection info
    follow_the_leader.master_lock_changed(None)
    assert follow_the_leader._am_following == None
    follow_the_leader.master_lock_changed('not me')
    assert follow_the_leader._am_following == None
    follow_the_leader.master_lock_changed(follow_the_leader.app.my_id)
    assert follow_the_leader._am_following == None
