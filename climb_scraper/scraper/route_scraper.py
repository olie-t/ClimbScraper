from climb_scraper.scraper.scraper_functions import get_crag_id, url_builder, get_data, data_to_string, \
    string_processor_climbs, climbs_dataframe_creation, string_processor_grades, grades_dataframe_creation, \
    climbs_grades_dataframe_merge

if __name__ == '__main__':
    crag_id = get_crag_id("crag_ids.txt")
    print(f"Got crag_id: {crag_id}")
    crag_url = url_builder(crag_id)

    print(f"Got crag_url: {crag_url}")
    url_data = get_data(crag_url)
    if url_data == None:
        print("Failed to get data from crag_url")
    print(f"Got URL data")
    data_string = data_to_string(url_data)
    print("Stringified response data")
    climb_json = string_processor_climbs(data_string)
    print("Data processed and relevant json found")
    climb_dataframe = climbs_dataframe_creation(climb_json)
    print("Climbing data put into dataframe")
    grades_json = string_processor_grades(data_string)
    print("Grades data put into dataframe")
    grades_dataframe = grades_dataframe_creation(grades_json)
    print("Grades data put into dataframe")
    climbs_w_grades_df = climbs_grades_dataframe_merge(climb_dataframe, grades_dataframe)
    print(climbs_w_grades_df)


