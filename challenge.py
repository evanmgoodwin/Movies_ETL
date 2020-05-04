def etl(wiki, kaggle, ratings):
    
    # Import dependencies
    import json
    import pandas as pd
    import numpy as np
    import re
    from sqlalchemy import create_engine
    from config import db_password
    import psycopg2
    import time
    
####################################################################################################################
    
    # Define the file directory
    file_dir = "C:/Users/evanm/Desktop/Classwork/Module 8 - Movies_ETL/Movies_ETL/"

    # Save the data as a list of dictionaries and convert to dataframe
    with open(f"{file_dir}wikipedia.movies.json", mode="r") as file:
        wiki_movies_raw = json.load(file)
    
    # List comprehension for director, imdb link and episodes
    wiki_movies = [movie for movie in wiki_movies_raw
                if ("Director" in movie or "Directed by" in movie)
                  and "imdb_link" in movie
                  and "No. of episodes" not in movie]
    
    # Load the kaggle data into a dataframe
    kaggle_data = pd.read_csv(f"{file_dir}kaggle_metadata.csv", low_memory=False)

####################################################################################################################
    
    # Clean movie and merge column function
    def clean_movie(movie):
    
        # Create a non-destructive copy
        movie = dict(movie)
        # Create an empty dictionary to hold alternate titles
        alt_titles = {}
        # Loop through list of alternate title keys
        for key in ["Also known as", "Arabic", "Cantonese", "Chinese", "French",
                   "Hangul", "Hebrew", "Hepburn", "Japanese", "Literally",
                   "Mandarin", "McCune–Reischauer", "Original title", "Polish",
                   "Revised Romanization", "Romanized", "Russian",
                   "Simplified", "Traditional", "Yiddish"]:
            # If an alternate title exists, remove it and add to alt_titles
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        # Add the alt_titles dictionary to the movie object
        if len(alt_titles) > 0:
            movie["alt_titles"] = alt_titles
        
        # Merge column names with inner function
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
    
        change_column_name("Adaptation by", "Writer(s)")
        change_column_name("Country of origin", "Country")
        change_column_name("Created by", "Creator")
        change_column_name("Directed by", "Director")
        change_column_name("Distributed by", "Distributor")
        change_column_name("Edited by", "Editor(s)")
        change_column_name("Length", "Running Time")
        change_column_name("Running time", "Running Time")
        change_column_name("Original release", "Release date")
        change_column_name("Music by", "Composer(s)")
        change_column_name("Produced by", "Producer(s)")
        change_column_name("Producer", "Producer(s)")
        change_column_name("Productioncompanies ", "Production company(s)")
        change_column_name("Productioncompany ", "Production company(s)")
        change_column_name("Released", "Release date")
        change_column_name("Release Date", "Release date")
        change_column_name("Screen story by", "Writer(s)")
        change_column_name("Screenplay by", "Writer(s)")
        change_column_name("Story by", "Writer(s)")
        change_column_name("Theme music composer", "Composer(s)")
        change_column_name("Written by", "Writer(s)")
    
        return movie

####################################################################################################################
    
    # List comprehension with updated clean_movie function
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    
    
    # Extract the imdb ids from the imdb_link and put them in a new column, and drop any duplicates
    wiki_movies_df["imdb_id"] = wiki_movies_df["imdb_link"].str.extract(r"(tt\d{7})")
    wiki_movies_df.drop_duplicates(subset="imdb_id", inplace=True)
    
    # Replace all columns in the dataframe
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    
####################################################################################################################
    
    # Create a function to parse monetary formats
    def parse_dollars(s):
    
        # If s is not a string, return NaN
        if type(s) !=str:
            return np.nan
    
        # If input is of the form $###.# million
        if re.match(r"\$\s*\d+\.?\d*\s*milli?on", s, flags=re.IGNORECASE):
            # Remove dollar sign and "million"
            s = re.sub("\$|\s|[a-zA-Z]", "", s)
            # Convert to float and mulitply by a million
            value = float(s) * 10**6
            # Return value
            return value
    
        # If input is of the form $###.# billion
        elif re.match(r"\$\s*\d+\.?\d*\s*billi?on", s, flags=re.IGNORECASE):
            # Remove dollar sign and "billion"
            s = re.sub("\$|\s|[a-zA-Z]", "", s)
            # Convert to float and multiply by a billion
            value = float(s) * 10**9
            # Return value
            return value
    
        # If input is of the form $###,###,###
        elif re.match(r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)", s, flags=re.IGNORECASE):
            # Remove dollar sign and commas
            s = re.sub("\$|,", "", s)
            # Convert to float
            value = float(s)
            # Return value
            return value
    
        # Otherwise, return NaN
        else:
            return np.nan
    
####################################################################################################################    
    
    ### Box Office / Budget Data###
    
    # Create a box office variable for nun_null box office values and convert lists to strings
    box_office = wiki_movies_df["Box office"].dropna().apply(lambda x: " ".join(x) if type(x) == list else x)

    # Create a variable for the first regular expression string
    form_one = r"\$\s*\d+\.?\d*\s*[mb]illi?on"
    
    # Create a variable for the second regular expression string
    form_two = r"\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)"
    
    # Replace hyphens in box office values
    box_office = box_office.str.replace(r"\$.*[-—–](?![a-z])", "$", regex=True)

    # Extract the box office values and pass through the parse dollars function, and remove the original box office column
    wiki_movies_df["box_office"] = box_office.str.extract(f"({form_one}|{form_two})", flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop("Box office", axis=1, inplace=True)
    
####################################################################################################################
    
    ### Budget ###
    
    # Create a budget variable for nun-null budget values and convert lists to strings
    budget = wiki_movies_df["Budget"].dropna().apply(lambda x: " ".join(x) if type(x) == list else x)
    
    # Remove any values between a dollar sign and a hyphen (ranges)
    budget = budget.str.replace(r"\$.*[-—–](?![a-z])", "$", regex=True)
    
    # Remove instances of [] in the budget values
    budget = budget.str.replace(r"\[\d+\]\s*", "")
    
    # Extract the budget values and pass through the parse dollars function
    wiki_movies_df["budget"] = budget.str.extract(f"({form_one}|{form_two})", flags=re.IGNORECASE)[0].apply(parse_dollars)

    # Drop the original budget column
    wiki_movies_df.drop("Budget", axis=1, inplace=True)
    
####################################################################################################################
    
    ## Running Time ###
    
    # Assign a variable to non null running time values and convert lists to strings
    running_time = wiki_movies_df["Running Time"].dropna().apply(lambda x: " ".join(x) if type(x) == list else x)
    
    # Extract only digits and allow both patterns
    running_time_extract = running_time.str.extract(r"(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m")
    
    # Convert running_time_extract values from strings to numeric values, convert empty strings to nan, and convert nans to zeros
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors="coerce")).fillna(0)
    
    # Convert hour values to minutes if current minutes equals zero
    wiki_movies_df["running_time"] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    
    # Drop the original running time column
    wiki_movies_df.drop("Running Time", axis=1, inplace=True)
    
####################################################################################################################
    
    # Keep rows where adult column is false, and drop the adult column
    kaggle_data = kaggle_data[kaggle_data["adult"] == "False"].drop("adult", axis="columns")
    
    # Convert budget, id and populartiy column values from objects to numeric
    kaggle_data["budget"] = kaggle_data["budget"].astype(int)
    kaggle_data["id"] = pd.to_numeric(kaggle_data["id"], errors="raise")
    kaggle_data["popularity"] = pd.to_numeric(kaggle_data["popularity"], errors="raise")
    
    
    # Merge wiki movies and movies metdata dataframes
    movies_df = pd.merge(wiki_movies_df, kaggle_data, on="imdb_id", suffixes=["_wiki", "_movies"])
    
    # Convert release date column values from objects to numeric
    kaggle_data["release_date"] = pd.to_datetime(kaggle_data["release_date"])
    
    # Drop title_wiki, Release Date, Language, Production company(s) and video columns
    movies_df.drop(columns=["title_wiki", "Release date", "Language", "Production company(s)", "video"], inplace=True)
    
    
####################################################################################################################    
    
    # Create a function that fills missing data in a column pair and drops the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column],
            axis=1)
        df.drop(columns=wiki_column, inplace=True)
    
    # Pass the three columns we're replacing zeros with wiki data through above function
    fill_missing_kaggle_data(movies_df, "runtime", "running_time")
    fill_missing_kaggle_data(movies_df, "budget_movies", "budget_wiki")
    fill_missing_kaggle_data(movies_df, "revenue", "box_office")
    
####################################################################################################################
    
    # Reorder the columns in movies_df
    movies_df = movies_df.loc[:, ['url', 'year', 'imdb_link', 'Based on', 'Starring', 'Cinematography', 'Country', 
                                  'Director', 'Distributor', 'Editor(s)', 'Composer(s)', 'Producer(s)', 'Writer(s)', 
                                  'imdb_id', 'belongs_to_collection', 'budget_movies', 'genres', 'homepage', 'id', 
                                  'original_language', 'original_title', 'overview', 'popularity', 'poster_path', 
                                  'production_companies', 'production_countries', 'release_date', 'revenue', 
                                  'runtime', 'spoken_languages', 'status', 'tagline', 'title_movies', 'vote_average', 
                                  'vote_count']]
    
    # Rename some of the columns
    movies_df.rename({"title_movies" : "title",
                     "id" : "kaggle_id",
                     "url" : "wikipedia_url",
                     "Based on" : "based_on",
                     "Starring" : "starring",
                     "Writer(s)" : "writers",
                     "Producer(s)" : "producers",
                     "Editor(s)" : "editors",
                     "Composer(s)" : "composers",
                     "Cinematography" : "cinematography",
                     "Country" : "country",
                     "Director" : "director",
                     "budget_movies" : "budget",
                     "Distributor" : "distributor"},
                     axis="columns", inplace=True)
    
####################################################################################################################  
    
    # Create the connection string and database engine to connect to database server
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    
    # Export movies_df to sql
    movies_df.to_sql(name="movies", con=engine, if_exists="replace")
    
####################################################################################################################
    
    ### Export ratings csv to sql database ###
    
    # Create a variable for the number of rows imported
    rows_imported = 0

    # Get the start_time from time.time()
    start_time = time.time()

    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
        
        # Print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    
        data.to_sql(name='ratings', con=engine, if_exists="replace")
    
        # Increment the number of rows imported by the size of "data"
        rows_imported += len(data)

        # Print that the rows have finished importing and add elapsed time
        print(f'Done. {time.time() - start_time} total seconds elapsed')
    
####################################################################################################################
    
    return