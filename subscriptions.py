query1 = """
   SELECT results.activity_date, results.user_id, results.signup_date, sum(results.number_of_activities) as number_of_activities
   FROM (SELECT date(events.updated_at) as activity_date, users.id AS user_id, date(users.created_at) AS signup_date,
   count(users.id) as number_of_activities
   FROM events
   INNER JOIN users on events.creator_id = users.id
   GROUP BY activity_date, users.id
   UNION
   SELECT date(event_instances.created_at) AS activity_date, users.id AS user_id,  date(users.created_at) AS signup_date,
   count(users.id) AS number_of_activities
   FROM event_instances
   INNER JOIN users on event_instances.user_id = users.id
   GROUP BY activity_date, users.id
   UNION
   SELECT date(follows.created_at) AS activity_date, users.id AS user_id, date(users.created_at) AS signup_date,
   count(users.id) as number_of_activities
   FROM follows
   INNER JOIN users on follows.follower_id = users.id
   AND follows.follower_type = 'User'
   GROUP BY activity_date, users.id) AS results
   GROUP BY results.activity_date, results.user_id, results.signup_date"""

query= """
SELECT quarter_signed_up, year_signed_up, AVG(days_to_first_subscribe) as average_days_to_subscribe, quarter_signed_up - 1 + 4*(year_signed_up-2013) as quarters_counted_from_january_2013_to_user_signing_up
FROM (SELECT result.user_signs_up, result.first_value as user_first_subscribes, EXTRACT(QUARTER FROM result.user_signs_up) as quarter_signed_up, EXTRACT(YEAR FROM result.user_signs_up) as year_signed_up,
CASE WHEN result.user_signs_up::date > '2016-5-31'::date THEN result.first_value::date - result.user_signs_up::date
ELSE result.first_value::date - '2016-5-31'::date
END as days_to_first_subscribe
FROM (SELECT users.id as user_id, users.created_at as user_signs_up, first_value(subscriptions.started_at) OVER (PARTITION BY user_id)
FROM subscriptions
INNER JOIN users on subscriptions.user_id = users.id
WHERE subscriptions.sponsor_id = users.id OR subscriptions.sponsor_id IS null
ORDER BY subscriptions.started_at ASC) as result) as resultant
GROUP BY quarter_signed_up, year_signed_up
ORDER BY year_signed_up, quarter_signed_up ASC
"""

import pandas as pd

postgreurl = 'postgresql://username:password@localhost:5423/dbname'

d = pd.read_sql(query, postgreurl)
d.plot.scatter(x='quarters_counted_from_january_2013_to_user_signing_up', y = 'average_days_to_subscribe', xticks = range(20)).plot()
import matplotlib.pyplot as plt
plt.show()
