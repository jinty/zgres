def test_integer_wal_pos():
    from ..ec2 import _integer_wal_pos
    assert _integer_wal_pos('67E/AFE198') - _integer_wal_pos('67D/FECFA308') == 14696080
    assert _integer_wal_pos('0/000000') == 0
    assert _integer_wal_pos('0/00000F') == 15
    assert _integer_wal_pos('1/00000F') == 0xFF00000F
