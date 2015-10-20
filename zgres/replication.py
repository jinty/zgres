from .plugin import subscribe

class FollowTheLeader:

    _am_following = None
    _current_master = None
    _current_conn_info = None
    
    def __init__(self, name, app):
        self.name = name
        self.app = app

    @subscribe
    def master_lock_changed(self, owner):
        self._current_master = owner
        self._check_following()
        
    @subscribe
    def conn_info(self, connection_info):
        self._current_conn_info = connection_info
        self._check_following()

    def _check_following(self):
        if self._current_master == self._am_following:
            return
        if self._current_conn_info is not None and self._current_master in self._current_conn_info:
            master_info = self._current_conn_info.get(self._current_master)
            self.app.follow(dict(
                host=master_info['host'],
                port=master_info.get('port', '5432')))
            self._am_following = self._current_master
