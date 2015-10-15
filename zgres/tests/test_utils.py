def test_pg_lsn_to_int():
    from ..utils import pg_lsn_to_int
    assert pg_lsn_to_int('67E/AFE198') - pg_lsn_to_int('67D/FECFA308') == 14696080
    assert pg_lsn_to_int('0/000000') == 0
    assert pg_lsn_to_int('0/00000F') == 15
    assert pg_lsn_to_int('1/00000F') == 0xFF00000F
