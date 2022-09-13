from helpers.weather_pipeline_helpers.helpers import create_pg_engine 



def test_create_pg_engine():

    engine = create_pg_engine()

    assert engine is not None




