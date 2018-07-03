import pandas as pd

postgreurl = 'postgresql://username:password@localhost:5423/dbname'

query = """
WITH subscription_records AS
(SELECT users.created_at as join_date, subscriptions.user_id as user_id, subscriptions.started_at as started_at, subscriptions.ended_at as ended_at, plans.amount as amount, subscriptions.sponsor_id as sponsor_id
FROM subscriptions
INNER JOIN plans on subscriptions.plan_id = plans.id
INNER JOIN users on subscriptions.user_id = users.id)

SELECT COUNT(quarter_signed_up) as number_of_upgrades, quarter_signed_up - 1 + 4*(year_signed_up-2013) as quarters_counted_from_january_2013_to_user_signing_up
FROM (SELECT EXTRACT(QUARTER FROM result.join_date) as quarter_signed_up, EXTRACT(YEAR FROM result.join_date) as year_signed_up
FROM (SELECT t1.started_at::date as upgrade_date, t1.user_id as user_id, t1.join_date::date as join_date
FROM subscription_records t1, subscription_records t2
WHERE t1.user_id = t2.user_id
AND t1.amount > t2.amount
AND t1.started_at::date = t2.ended_at::date
AND (t1.sponsor_id = t1.user_id OR t1.sponsor_id IS null)) as result) resultant
GROUP BY quarter_signed_up, year_signed_up
ORDER BY quarter_signed_up, year_signed_up ASC
"""

d = pd.read_sql(query, postgreurl)
d.plot.scatter(x='quarters_counted_from_january_2013_to_user_signing_up', y = 'number_of_upgrades', xticks = range(20)).plot()
import matplotlib.pyplot as plt
plt.show()

query = """
WITH subscription_records AS
(SELECT users.created_at as join_date, subscriptions.user_id as user_id, subscriptions.started_at as started_at, subscriptions.ended_at as ended_at, plans.amount as amount, subscriptions.sponsor_id as sponsor_id
FROM subscriptions
INNER JOIN plans on subscriptions.plan_id = plans.id
INNER JOIN users on subscriptions.user_id = users.id)

SELECT COUNT(quarter_signed_up) as number_of_downgrades, quarter_signed_up - 1 + 4*(year_signed_up-2013) as quarters_counted_from_january_2013_to_user_signing_up
FROM (SELECT EXTRACT(QUARTER FROM resultant.join_date) as quarter_signed_up, EXTRACT(YEAR FROM resultant.join_date) as year_signed_up
FROM (SELECT t1.started_at::date as downgrade_date, t1.user_id as user_id, t1.join_date::date as join_date
FROM subscription_records t1, subscription_records t2
WHERE t1.user_id = t2.user_id
AND t1.amount < t2.amount
AND t1.started_at::date = t2.ended_at::date
AND (t1.sponsor_id = t1.user_id OR t1.sponsor_id IS null)

UNION

SELECT t3.ended_at::date as downgrade_date, t3.user_id as user_id, t3.join_date::date as join_date
FROM subscription_records t3
WHERE t3.ended_at IS NOT null
AND
NOT EXISTS (SELECT *
        FROM subscription_records t4
        WHERE
            t4.started_at = t3.ended_at AND t4.user_id = t3.user_id)) resultant) resultant2
GROUP BY quarter_signed_up, year_signed_up
ORDER BY quarter_signed_up, year_signed_up ASC
"""

d = pd.read_sql(query, postgreurl)
d.plot.scatter(x='quarters_counted_from_january_2013_to_user_signing_up', y = 'number_of_downgrades', xticks = range(20)).plot()
import matplotlib.pyplot as plt
plt.show()

query = """
WITH subscription_records AS
(SELECT users.created_at as join_date, subscriptions.user_id as user_id, subscriptions.started_at as started_at, subscriptions.ended_at as ended_at, plans.amount as amount, subscriptions.sponsor_id as sponsor_id
FROM subscriptions
INNER JOIN plans on subscriptions.plan_id = plans.id
INNER JOIN users on subscriptions.user_id = users.id)

SELECT AVG(days_to_downgrade) as average_days_to_downgrade, quarter_signed_up - 1 + 4*(year_signed_up-2013) as quarters_counted_from_january_2013_to_user_signing_up
FROM(SELECT EXTRACT(QUARTER FROM resultant.join_date) as quarter_signed_up, EXTRACT(YEAR FROM resultant.join_date) as year_signed_up, resultant.days_to_downgrade as days_to_downgrade
FROM(SELECT result.user_id, result.join_date, result.downgrade_date - result.previous_subscription_starting_date  as days_to_downgrade
FROM (SELECT t1.started_at::date as downgrade_date, t1.user_id as user_id, t1.join_date::date as join_date, t2.started_at::date as previous_subscription_starting_date
FROM subscription_records t1, subscription_records t2
WHERE t1.user_id = t2.user_id
AND t1.amount < t2.amount
AND t1.started_at::date = t2.ended_at::date
AND (t1.sponsor_id = t1.user_id OR t1.sponsor_id IS null)

UNION

SELECT t3.ended_at::date as downgrade_date, t3.user_id as user_id, t3.join_date::date as join_date, t3.started_at::date as previous_subscription_starting_date
FROM subscription_records t3
WHERE t3.ended_at IS NOT null
AND
NOT EXISTS (SELECT *
        FROM subscription_records t4
        WHERE
            t4.started_at = t3.ended_at AND t4.user_id = t3.user_id)) result) resultant) resultant2
GROUP BY year_signed_up, quarter_signed_up
ORDER BY year_signed_up, quarter_signed_up ASC

"""

d = pd.read_sql(query, postgreurl)
d.plot.scatter(x='quarters_counted_from_january_2013_to_user_signing_up', y = 'average_days_to_downgrade', xticks = range(20)).plot()
import matplotlib.pyplot as plt
plt.show()

query = """
WITH subscription_records AS
(SELECT users.created_at as join_date, subscriptions.user_id as user_id, subscriptions.started_at as started_at, subscriptions.ended_at as ended_at, plans.amount as amount, subscriptions.sponsor_id as sponsor_id
FROM subscriptions
INNER JOIN plans on subscriptions.plan_id = plans.id
INNER JOIN users on subscriptions.user_id = users.id)

SELECT AVG(days_to_upgrade) as average_days_to_upgrade, quarter_signed_up - 1 + 4*(year_signed_up-2013) as quarters_counted_from_january_2013_to_user_signing_up
FROM (SELECT EXTRACT(QUARTER FROM result.join_date) as quarter_signed_up, EXTRACT(YEAR FROM result.join_date) as year_signed_up, result.days_to_upgrade as days_to_upgrade
FROM (SELECT t1.started_at::date as upgrade_date, t1.user_id as user_id, t1.join_date::date as join_date, t1.started_at::date - t2.started_at::date as days_to_upgrade
FROM subscription_records t1, subscription_records t2
WHERE t1.user_id = t2.user_id
AND t1.amount > t2.amount
AND t1.started_at::date = t2.ended_at::date
AND (t1.sponsor_id = t1.user_id OR t1.sponsor_id IS null)) as result) as resultant
GROUP BY quarter_signed_up, year_signed_up
ORDER BY quarter_signed_up, year_signed_up ASC

"""
d = pd.read_sql(query, postgreurl)
d.plot.scatter(x='quarters_counted_from_january_2013_to_user_signing_up', y = 'average_days_to_upgrade', xticks = range(20)).plot()
plt.show()
