import sqlite3


class ChainTask:
    def __init__(self, chain_id=0, current_task=0, next_task=0, order_by=0):
        self.chain_id = chain_id
        self.current_task = current_task
        self.next_task = next_task
        self.order_by = order_by

    def __str__(self):
        return f"ChainTask [chainId={self.chain_id}, currentTask={self.current_task}, NextTask={self.next_task}, orderBy={self.order_by}]"


class JDBCUtility:
    # Singleton pattern for getting a database connection
    instance = None

    @staticmethod
    def get_instance():
        if not JDBCUtility.instance:
            JDBCUtility.instance = JDBCUtility()
        return JDBCUtility.instance

    def get_connection(self):
        # For demonstration purposes, I'm connecting to an SQLite database.
        # You should replace this with your database connection logic.
        conn = sqlite3.connect('your_database_name.db')
        return conn


class Logic:
    NU_TASK_LIST = [2136, 2137, 2139, 2140, 3136, 3137, 3139, 3140, 4136, 4137, 4139, 4140]

    @staticmethod
    def get_chain_task_not_in_used():
        conn = JDBCUtility.get_instance().get_connection()
        list_tasks = []

        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM empdb.chain_task WHERE CurrentTask IN (2136,2137,2139,2140,3136,3137,3139,3140,4136,4137,4139,4140) OR NextTask IN (2136,2137,2139,2140,3136,3137,3139,3140,4136,4137,4139,4140)")

            for row in cursor.fetchall():
                chain_task = ChainTask(row[0], row[1], row[2], row[3])
                list_tasks.append(chain_task)

            cursor.close()
            conn.close()

        except Exception as e:
            print(e)

        finally:
            conn.close()

        return list_tasks

    def get_list_of_chain_task_details(self, chain_id):
        conn = JDBCUtility.get_instance().get_connection()
        list_task = []

        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM empdb.chain_task WHERE ChainID = {chain_id}")

            for row in cursor.fetchall():
                chain_task = ChainTask(row[0], row[1], row[2], row[3])
                list_task.append(chain_task)

            cursor.close()
            conn.close()

        except Exception as e:
            print(e)

        print("getListOfChainTaskDetails ::", list_task)
        return list_task


def update_the_order(self, chain_id):
    conn = JDBCUtility.get_instance().get_connection()
    list_tasks = []

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM empdb.chain_task WHERE ChainID = {chain_id} ORDER BY OrderBy ASC")

        order_by = 1
        for row in cursor.fetchall():
            chain_task = ChainTask(row[0], row[1], row[2], row[3])

            if row[3] != order_by:
                update_conn = JDBCUtility.get_instance().get_connection()
                sql1 = f"UPDATE empdb.chain_task SET OrderBy = {order_by} WHERE ChainID = {chain_id} AND CurrentTask = {row[1]} AND NextTask = {row[2]}"
                print("SQL Update Query ::", sql1)
                update_cursor = update_conn.cursor()
                update_cursor.execute(sql1)
                update_conn.close()

                chain_task.order_by = order_by
            else:
                chain_task.order_by = row[3]

            list_tasks.append(chain_task)
            order_by += 1

    except Exception as e:
        print(e)

    finally:
        conn.close()

    return list_tasks


def update_and_delete(self):
    self.delete_chain_which_current_and_next_task_not_in_used()

    # Assuming get_chain_task_not_in_used() is defined elsewhere based on the previous answer
    list_tasks = self.get_chain_task_not_in_used()

    chain_id_set = set(task.chain_id for task in list_tasks)

    conn = JDBCUtility.get_instance().get_connection()

    try:
        for chain_id in chain_id_set:
            tasks = self.update_the_order(chain_id)
            no_used_list = [task for task in list_tasks if task.chain_id == chain_id]

            previous_task = None
            for idx, CTL in enumerate(tasks):
                if idx > 0:
                    previous_task = tasks[idx - 1]

                for NUL in no_used_list:
                    if CTL.current_task == NUL.current_task or CTL.next_task == NUL.next_task:
                        if previous_task.next_task in Logic.NU_TASK_LIST and CTL.current_task in Logic.NU_TASK_LIST:
                            conn2 = JDBCUtility.get_instance().get_connection()
                            sql1 = f"UPDATE empdb.chain_task SET NextTask = {CTL.next_task} WHERE ChainID = {chain_id} AND CurrentTask = {previous_task.current_task} AND NextTask = {previous_task.next_task}"
                            delete_sql = f"DELETE FROM empdb.chain_task WHERE ChainID = {chain_id} AND CurrentTask = {CTL.current_task} AND NextTask = {CTL.next_task}"
                            cursor = conn2.cursor()
                            cursor.execute(sql1)
                            cursor.execute(delete_sql)
                            conn2.close()

    except Exception as e:
        print(e)

    finally:
        conn.close()


def delete_chain_which_current_and_next_task_not_in_used(self):
    conn = JDBCUtility.get_instance().get_connection()
    try:
        sql = "DELETE FROM empdb.chain_task WHERE CurrentTask IN (2136,2137,2139,2140,3136,3137,3139,3140,4136,4137,4139,4140) AND NextTask IN (2136,2137,2139,2140,3136,3137,3139,3140,4136,4137,4139,4140)"
        cursor = conn.cursor()
        cursor.execute(sql)
    except Exception as e:
        print(e)
    finally:
        conn.close()


if __name__ == "__main__":
    l = Logic()
    l.update_and_delete()
