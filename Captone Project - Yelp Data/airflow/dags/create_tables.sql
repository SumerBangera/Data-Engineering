-- STAGING TABLES
CREATE TABLE IF NOT EXISTS staging_reviews(
                                            review_id         CHAR (22),
                                            user_id           CHAR (22),
                                            business_id       CHAR (22),
                                            stars             NUMERIC,
                                            date              DATE,
                                            review_text       VARCHAR,  
                                            useful            INTEGER,
                                            funny             INTEGER,
                                            cool              INTEGER
);
    
CREATE TABLE IF NOT EXISTS staging_restaurants(
                                                business_id         CHAR (22),
                                                business_name       VARCHAR,
                                                address             VARCHAR,
                                                city                VARCHAR,
                                                state               VARCHAR,
                                                postal_code         VARCHAR,
                                                latitude            NUMERIC,
                                                longitude           NUMERIC,
                                                stars               NUMERIC,
                                                review_count        INTEGER,       -- VARCHAR,
                                                categories          VARCHAR          --TEXT[] implies an array of texts
                                                    
);

CREATE TABLE IF NOT EXISTS staging_users(
                                          user_id               CHAR (22),
                                          user_name             VARCHAR,
                                          review_count          INTEGER,
                                          yelping_since         DATE,
                                          useful                INTEGER,
                                          funny                 INTEGER,
                                          cool                  INTEGER,
                                          fans                  INTEGER,
                                          average_stars         NUMERIC,
                                          compliment_hot        INTEGER,
                                          compliment_more       INTEGER,
                                          compliment_profile    INTEGER,
                                          compliment_cute       INTEGER,
                                          compliment_list       INTEGER,
                                          compliment_note       INTEGER,
                                          compliment_plain      INTEGER,
                                          compliment_cool       INTEGER,
                                          compliment_funny      INTEGER,
                                          compliment_writer     INTEGER,
                                          compliment_photos     INTEGER
);
    
CREATE TABLE IF NOT EXISTS staging_checkin(
                                            business_id         CHAR (22),
                                            count               INTEGER,
                                            min                 DATE,
                                            max                 DATE
);

-- Final tables


--  Dimension Tables
CREATE TABLE IF NOT EXISTS restaurants(
                                    business_id        CHAR (22) PRIMARY KEY NOT NULL,
                                    business_name     VARCHAR,
                                    city               VARCHAR,
                                    state              VARCHAR, 
                                    postal_code        VARCHAR,
                                    latitude           NUMERIC,
                                    longitude          NUMERIC,
                                    stars              NUMERIC,
                                    categories         VARCHAR,   -- TEXT[]not working
                                    review_count       INTEGER,   -- VARCHAR,
                                    total_checkins     INTEGER,
                                    first_checkin      DATE,
                                    latest_checkin     DATE
);

CREATE TABLE IF NOT EXISTS users(
                                  user_id               CHAR (22)   PRIMARY KEY NOT NULL,
                                  user_name             VARCHAR,
                                  review_count_user     INTEGER,
                                  yelping_since         DATE,
                                  useful_total          INTEGER,
                                  funny_total           INTEGER,
                                  cool_total            INTEGER,
                                  fans                  INTEGER,
                                  average_stars         NUMERIC,
                                  compliment_hot        INTEGER,
                                  compliment_more       INTEGER,
                                  compliment_profile    INTEGER,
                                  compliment_cute       INTEGER,
                                  compliment_list       INTEGER,
                                  compliment_note       INTEGER,
                                  compliment_plain      INTEGER,
                                  compliment_cool       INTEGER,
                                  compliment_funny      INTEGER,
                                  compliment_writer     INTEGER,
                                  compliment_photos     INTEGER
);

CREATE TABLE IF NOT EXISTS date_table(
                                       date        DATE       PRIMARY KEY NOT NULL,
                                       day         INTEGER,
                                       week        VARCHAR,
                                       month       INTEGER,
                                       year        INTEGER,
                                       dayOfWeek   VARCHAR
);

--  Fact Table
CREATE TABLE IF NOT EXISTS reviews(
                                    review_id         CHAR (22)   PRIMARY KEY NOT NULL,
                                    user_id           CHAR (22)   REFERENCES users(user_id) NOT NULL,
                                    business_id       CHAR (22)   REFERENCES restaurants(business_id) NOT NULL,
                                    review_date       DATE        REFERENCES date_table(date) NOT NULL,
                                    review_text       VARCHAR,  
                                    stars_given       NUMERIC,
                                    useful            INTEGER,
                                    funny             INTEGER,
                                    cool              INTEGER
);