class SqlQueries:
    # """Helper function to create and insert values into tables"""
    reviews_table_insert = ("""SELECT DISTINCT
                                        rv.review_id,
                                        rv.user_id,
                                        rv.business_id,
                                        rv.date AS review_date,
                                        rv.review_text,
                                        rv.stars AS stars_given,
                                        rv.useful,
                                        rv.funny,
                                        rv.cool
                               FROM staging_reviews rv
    """)

    restaurants_table_insert = ("""SELECT DISTINCT
                                          r.business_id,
                                          r.business_name,
                                          r.city,
                                          r.state,
                                          r.postal_code,
                                          r.latitude,
                                          r.longitude,
                                          r.stars,
                                          r.categories,
                                          r.review_count,
                                          c.count AS total_checkins,
                                          c.min AS first_checkin,
                                          c.max AS latest_checkin
                                   FROM staging_restaurants r
                                   LEFT JOIN staging_checkin c
                                        ON r.business_id = c.business_id
          
    """)

    users_table_insert = ("""SELECT DISTINCT
                                    u.user_id,
                                    u.user_name AS user_name,
                                    u.review_count AS review_count_user,
                                    u.yelping_since,
                                    u.useful AS useful_total,
                                    u.funny AS funny_total,
                                    u.cool AS cool_total,
                                    u.fans,
                                    u.average_stars,
                                    u.compliment_hot,
                                    u.compliment_more,
                                    u.compliment_profile,
                                    u.compliment_cute,
                                    u.compliment_list,
                                    u.compliment_note,
                                    u.compliment_plain,
                                    u.compliment_cool,
                                    u.compliment_funny,
                                    u.compliment_writer,
                                    u.compliment_photos
                             FROM staging_users u
    """)

    date_table_insert = ("""SELECT 
                                 rv.date,
                                 extract(day from rv.date) AS day, 
                                 extract(week from rv.date) AS week, 
                                 extract(month from rv.date) AS month, 
                                 extract(year from rv.date) AS year, 
                                 extract(dayofweek from rv.date) AS dayOfWeek
                            FROM staging_reviews rv
    """)