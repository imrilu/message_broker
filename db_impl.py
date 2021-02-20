import sqlite3

SUCCESS = "success"
DB_LOCATION = './database.db'

connection = sqlite3.connect(DB_LOCATION, check_same_thread=False)
connection.isolation_level = None  # disable automatic commits to db
cursor = connection.cursor()


def init():
    cursor.execute("""CREATE TABLE IF NOT EXISTS topics(
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    subscribers TEXT);
                """)

    cursor.execute("""CREATE TABLE IF NOT EXISTS messages(
                    id INTEGER PRIMARY KEY,
                    client_id INTEGER,
                    topic_id INTEGER,
                    topic_name TEXT,
                    message TEXT,
                    FOREIGN KEY (topic_id) REFERENCES topics (id));
                    """)

    connection.commit()
    return connection, cursor


def clear_db():
    cursor.execute("""DROP TABLE IF EXISTS topics""")
    cursor.execute("""DROP TABLE IF EXISTS messages""")
    connection.commit()
    return SUCCESS


def create_topic(topic):
    result = cursor.execute("""SELECT * FROM topics WHERE name = ?;""", (topic,))
    topic_row = result.fetchall()
    if topic_row:
        raise Exception("create_topic: topic already exist")
    cursor.execute("""INSERT INTO topics(name, subscribers) VALUES(?, ?);""", (topic, ""))
    connection.commit()
    return SUCCESS


def get_topic(topic):
    result = cursor.execute("""SELECT * FROM topics WHERE name = ?;""", (topic,))
    topic_row = result.fetchall()
    if topic_row:
        return topic_row[0]
    else:
        raise Exception("no topic exist with that name")


def subscribe(id, topic):
    topic_info = get_topic(topic)
    subs = add_sub(id, topic_info[2])
    if subs != topic_info[2]:
        cursor.execute("""UPDATE topics SET subscribers = ? WHERE id = ?;""", (subs, topic_info[0]))
        connection.commit()
    return SUCCESS


def unsubscribe(id, topic):
    topic_info = get_topic(topic)
    subs = remove_sub(id, topic_info[2])
    if subs != topic_info[2]:
        cursor.execute("""UPDATE topics SET subscribers = ? WHERE id = ?;""", (subs, topic_info[0]))
        connection.commit()
    return SUCCESS


def publish(msg, topic):
    topic_info = get_topic(topic)
    subs = topic_info[2].split(',')
    if subs:
        cursor.executemany("""INSERT INTO messages(client_id, topic_id, topic_name, message) VALUES(?, ?, ?, ?);""",
                           [(id, topic_info[0], topic_info[1], msg) for id in subs])
    connection.commit()
    return SUCCESS


def listen(id):
    try:
        cursor.execute("BEGIN")
        result = cursor.execute("""SELECT message FROM messages WHERE client_id = ?;""", (id,))
        messages = result.fetchall()
        if messages:
            cursor.execute("""DELETE FROM messages WHERE client_id = ?;""", (id,))
            cursor.execute("COMMIT")
            return [m[0] for m in messages]
        else:
            cursor.execute("COMMIT")
    except:
        cursor.execute("ROLLBACK")
        raise Exception("listen: error in SQL statement")


def remove_sub(sub, lst):
    if sub not in lst:
        return lst
    elif lst == sub:
        return lst.replace(sub, "")
    elif lst.endswith(sub):
        return lst.replace(',' + sub, "")
    else:
        return lst.replace(sub + ',', "")


def add_sub(sub, lst):
    if lst == "":
        return lst + sub
    elif sub in lst:
        return lst
    else:
        return lst + ',' + sub
