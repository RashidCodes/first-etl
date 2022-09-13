from helpers.weather_pipeline_helpers.helpers import generate_datetime_ranges



def test_generate_datetime_ranges():


    num_of_ranges = 2
    start_date = '2022-01-01' 
    end_date = '2022-01-03'
    generated_dates = generate_datetime_ranges(start_date, end_date) 

    assert num_of_ranges == len(generated_dates)


