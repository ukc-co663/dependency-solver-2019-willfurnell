import argparse
import json
import sys
from operator import *
import networkx as nx
import pymysql.cursors
from packaging import version as vparser
from z3 import Solver, Bool, Not, Or, And, unsat, unknown, Z3Exception, sat
import pycosat
import matplotlib.pyplot as plt

no_sql_notes = "SET sql_notes = 0"
if sys.platform == "darwin":
    cdbc = pymysql.connect(host='localhost', user='root', password='')
else:
    cdbc = pymysql.connect(unix_socket='/var/run/mysqld/mysqld.sock', user='root', password='')
cdbc.cursor().execute(no_sql_notes)
cdbc.cursor().execute("CREATE DATABASE IF NOT EXISTS depsolve")
cdbc.commit()

parser = argparse.ArgumentParser(description='Solve dependencies')
parser.add_argument('repo', metavar='r', type=str)
parser.add_argument('initial', metavar='i', type=str)
parser.add_argument('constraints', metavar='c', type=str)

args = parser.parse_args()

with open(args.repo, 'r') as repo_file:
    repository = json.load(repo_file)

with open(args.initial, 'r') as initial_file:
    initial = json.load(initial_file)

with open(args.constraints, 'r') as constraints_file:
    constraints = json.load(constraints_file)

if len(constraints) == 0:
    print(json.dumps([]))
    exit(0)


def make_conn():
    if sys.platform == "darwin":
        # Connect to the database
        conn = pymysql.connect(host='localhost',
                               user='root',
                               password='',
                               db='depsolve',
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
    else:
        # Connect to the database
        conn = pymysql.connect(unix_socket='/var/run/mysqld/mysqld.sock',
                               user='root',
                               password='',
                               db='depsolve',
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
    return conn


package_db = \
    '''
    CREATE TABLE packages (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(255),
        version VARCHAR(255),
        weight INTEGER,
        depends TEXT,
        conflicts TEXT
    );
    '''

conflicts_db = \
    """
    CREATE TABLE conflicts (
        package_id INTEGER,
        conflict_package_id INTEGER,
        PRIMARY KEY (package_id, conflict_package_id),
        FOREIGN KEY (package_id) REFERENCES packages(id),
        FOREIGN KEY (conflict_package_id) REFERENCES packages(id)
    );
    """

depends_db = \
    """
    CREATE TABLE depends (
        package_id INTEGER,
        depend_package_id INTEGER,
        must_be_installed INTEGER,
        opt_dep_group INTEGER,
        PRIMARY KEY (package_id, depend_package_id),
        FOREIGN KEY (package_id) REFERENCES packages(id),
        FOREIGN KEY (depend_package_id) REFERENCES packages(id)
    );
    """

state_db = \
    """
    CREATE TABLE state (
        package_id INTEGER,
        PRIMARY KEY (package_id), 
        FOREIGN KEY (package_id) REFERENCES packages(id)
    )
    """

unset_for_key_check = "SET foreign_key_checks = 0"
set_for_key_check = "SET foreign_key_checks = 1"
del_pkg = "DROP TABLE IF EXISTS packages, conflicts, depends, state"
del_everything_except_pkg = "DROP TABLE IF EXISTS conflicts, depends, state"

opt_dep_group = 0


def parse_constraints(constraints, order_by):
    installs = []
    uninstalls = []
    for constraint in constraints:
        if constraint[0] == "+":
            if "=" in constraint:
                const = constraint[1:].split("=")
                c.execute("SELECT id FROM packages WHERE name = %s AND version = %s", [const[0], const[1]])
                id = c.fetchone()
                installs.append(id['id'])
            else:
                c.execute("SELECT id FROM packages WHERE name = %s ORDER BY " + order_by, [constraint[1:]])
                id = c.fetchone()
                if id is not None:
                    installs.append(id['id'])
                else:
                    c.execute("SELECT id FROM packages WHERE name = %s ORDER BY weight ASC", [constraint[1:]])
                    id = c.fetchone()
                    installs.append(id['id'])
        else:
            if "=" in constraint:
                const = constraint[1:].split("=")
                c.execute("SELECT id FROM packages WHERE name = %s AND version = %s", [const[0], const[1]])
                id = c.fetchone()
                uninstalls.append(id['id'])
            else:
                c.execute("SELECT id FROM packages WHERE name = %s ORDER BY " + order_by, [constraint[1:]])
                id = c.fetchone()
                if id is not None:
                    uninstalls.append(id['id'])
                else:
                    c.execute("SELECT id FROM packages WHERE name = %s ORDER BY weight ASC", [constraint[1:]])
                    id = c.fetchone()
                    uninstalls.append(id['id'])

    return installs, uninstalls


def parse_vstring(version_string):
    if ">=" in version_string:
        return (version_string.split(">=")[0], version_string.split(">=")[1], ge)
    elif "<=" in version_string:
        return (version_string.split("<=")[0], version_string.split("<=")[1], le)
    elif "=" in version_string:
        return (version_string.split("=")[0], version_string.split("=")[1], eq)
    elif "<" in version_string:
        return (version_string.split("<")[0], version_string.split("<")[1], lt)
    elif ">" in version_string:
        return (version_string.split(">")[0], version_string.split(">")[1], gt)
    else:
        return version_string, None, None


def add_deps(pid, order_by):
    global opt_dep_group
    c.execute("SELECT depends FROM packages WHERE id = %s", [pid])
    depends = c.fetchone()
    depends = json.loads(depends['depends'])
    if len(depends) > 0:
        for dlist in depends:
            if len(dlist) == 1:
                must_be_installed = 1
            else:
                must_be_installed = 0
            for dep in dlist:
                package_name, package_version, package_req = parse_vstring(dep)
                if package_req is not None and package_version is not None:
                    c.execute("SELECT id, version, weight FROM packages WHERE name = %s ORDER BY " + order_by,
                              [package_name])
                    packages = c.fetchall()
                    if len(packages) != 0:
                        add_deps_versions_to_db(must_be_installed, opt_dep_group, packages, pid, package_req, package_version)
                    else:
                        c.execute("SELECT id, version, weight FROM packages WHERE name = %s ORDER BY weight ASC",
                                  [package_name])
                        packages = c.fetchall()
                        if len(packages) != 0:
                            add_deps_versions_to_db(must_be_installed, opt_dep_group, packages, pid, package_req, package_version)
                else:
                    c.execute("SELECT id, version, weight FROM packages WHERE name = %s ORDER BY " + order_by,
                              [package_name])
                    packages = c.fetchall()
                    if len(packages) != 0:
                        add_dep_to_db(must_be_installed, opt_dep_group, packages, pid)
                    else:
                        c.execute("SELECT id, version, weight FROM packages WHERE name = %s ORDER BY weight ASC",
                                  [package_name])
                        packages = c.fetchall()
                        if len(packages) != 0:
                            add_dep_to_db(must_be_installed, opt_dep_group, packages, pid)
            opt_dep_group += 1
    conn.commit()


def add_deps_versions_to_db(must_be_installed, opt_dep_group, packages, pid, package_req, package_version):
    packages_rightversion = filter(lambda x: package_req(vparser.parse(x['version']), vparser.parse(package_version)), packages)
    l = list(packages_rightversion)
    if len(l) > 0:
        add_dep_to_db(must_be_installed, opt_dep_group, l, pid)


def add_dep_to_db(must_be_installed, opt_dep_group, packages, pid):
    depid = packages[0]['id']
    try:
        c.execute(
            "INSERT INTO depends(package_id, depend_package_id, must_be_installed, opt_dep_group) VALUES (%s, %s, %s, %s)",
            [pid, depid, must_be_installed, opt_dep_group])
    except pymysql.IntegrityError:
        pass


def add_conflicts(pid):
    c.execute("SELECT conflicts FROM packages WHERE id = %s", [pid])
    conflicts = c.fetchone()
    conflicts = json.loads(conflicts['conflicts'])
    if len(conflicts) > 0:
        for conflict in conflicts:
            package_name, package_version, package_req = parse_vstring(conflict)
            if package_req is not None and package_version is not None:
                c.execute("SELECT id, version FROM packages WHERE name = %s", [package_name])
                cons = c.fetchall()
                for con in cons:
                    if package_req(vparser.parse(con['version']), vparser.parse(package_version)):
                        try:
                            c.execute("INSERT INTO conflicts(package_id, conflict_package_id) VALUES (%s, %s)",
                                      [pid, con['id']])
                        except pymysql.IntegrityError:
                            pass
            else:
                c.execute("SELECT id FROM packages WHERE name = %s", [package_name])
                cons = c.fetchall()
                for con in cons:
                    try:
                        c.execute("INSERT INTO conflicts(package_id, conflict_package_id) VALUES (%s, %s)",
                                  [pid, con['id']])
                    except pymysql.IntegrityError:
                        pass
    conn.commit()


def add_dep_to_installs(package_id, order_by, prev_package_id):
    if package_id not in seen:
        seen.append(package_id)
        add_deps(package_id, order_by)
        add_conflicts(package_id)

        add_conflict_to_uninstalls(package_id, order_by)
        G.add_node(package_id)
        if prev_package_id is not None:
            G.add_edge(prev_package_id, package_id)

        c.execute("SELECT depend_package_id FROM depends WHERE package_id = %s", [package_id])
        tmp = c.fetchall()
        if len(tmp) != 0:
            for d in tmp:
                add_dep_to_installs(d['depend_package_id'], order_by, package_id)


def add_conflict_to_uninstalls(package_id, order_by):
    add_conflicts(package_id)
    c.execute("SELECT conflict_package_id FROM conflicts WHERE package_id = %s", [package_id])
    tmp = c.fetchall()
    conflicts = []
    for con in tmp:
        G.add_node(con['conflict_package_id'])
        G.add_edge(package_id, con['conflict_package_id'])
        if con['conflict_package_id'] not in conflicts:
            conflicts.append(con['conflict_package_id'])
    map(lambda x: add_conflict_to_uninstalls(x, order_by), conflicts)


# Return the first "M" models of formula list of formulas F
# Copied from here: https://stackoverflow.com/questions/11867611/z3py-checking-all-solutions-for-equation
def get_models(s, M):
    result = []
    while len(result) < M and s.check() != unsat:
        m = s.model()
        result.append(m)
        # Create a new constraint the blocks the current model
        block = []
        for d in m:
            # create a constant from declaration
            c = d()
            block.append(c != m[d])
        s.add(Or(block))
    return result

conn = make_conn()

c = conn.cursor()
c.execute(no_sql_notes)
c.execute(unset_for_key_check)
c.execute(del_pkg)
c.execute(set_for_key_check)
c.execute(package_db)

conn.commit()

for p in repository:
    # Index repo packages by name and version
    c.execute("INSERT INTO packages(name, version, weight, depends, conflicts) VALUES (%s, %s, %s, %s, %s)",
              [p['name'], p['version'], p['size'], json.dumps(p['depends']) if 'depends' in p.keys() else "[]",
               json.dumps(p['conflicts']) if 'conflicts' in p.keys() else "[]"])

conn.commit()

sols = []
costs = []
order_bys = ['weight ASC', 'weight DESC', 'version ASC', 'version DESC', 'id DESC', 'weight ASC LIMIT 1,1', 'weight ASC LIMIT 2,1', 'weight ASC LIMIT 3,1', 'weight ASC LIMIT 4,1']

for order in order_bys:
    c.execute(conflicts_db)
    c.execute(depends_db)
    c.execute(state_db)
    conn.commit()
    c.execute("SELECT id, name FROM packages")
    ps = c.fetchall()
    #print(ps)

    G = nx.DiGraph()

    installs, uninstalls = parse_constraints(constraints, order)
    install_order = []
    install_order_ids = []
    state = []
    seen = []

    # Setup the state
    for i in initial:
        if "=" in i:
            name, version = i.split("=")
            c.execute("SELECT id FROM packages WHERE name = %s and version = %s", [name, version])
            res = c.fetchone()
            c.execute("INSERT INTO state(package_id) VALUES(%s)", [res['id']])
            state.append(res['id'])
        else:
            c.execute("SELECT id, weight FROM packages WHERE name = %s ORDER BY " + order, [i])
            ps = c.fetchall()
            pid = ps[0]['id']
            c.execute("INSERT INTO state(package_id) VALUES(%s)", [pid])
            state.append(pid)

    # Uninstalls from constraints
    for n in set(uninstalls):
        c.execute("SELECT name, version FROM packages, state WHERE id = %s AND package_id = %s", [n, n])
        res = c.fetchone()
        if res:
            install_order.append("-" + res['name'] + "=" + res['version'])
            install_order_ids.append(n)

    conn.commit()

    # Do everything basically
    ii, _ = parse_constraints(constraints, order)
    for i in ii:
        # print("Install: " + str(i))
        add_dep_to_installs(i, order, None)

    solver = Solver()

    var_mapping = {}
    node_groups = []

    cycles = nx.recursive_simple_cycles(G)
    for cycle in cycles:
        G.remove_nodes_from(cycle[1:])

    var_groups = {}
    cnf = []

    for n in nx.algorithms.dag.topological_sort(G):
        direct_descendants = G[n].keys()
        predecessors = sum(1 for _ in G.predecessors(n))

        if predecessors == 0:
            v = Bool(n)
            solver.add(v)
            var_mapping[n] = v

        for descendant in direct_descendants:

            c.execute("SELECT * FROM depends WHERE package_id = %s AND depend_package_id = %s", [n, descendant])
            descendant_depend_info = c.fetchone()
            c.execute("SELECT COUNT(conflict_package_id) as c FROM conflicts WHERE conflict_package_id = %s AND package_id = %s", [descendant, n])
            conflict_count = c.fetchone()['c']

            if conflict_count > 0:
                v = Bool(descendant)
                solver.add(Not(v))
                var_mapping[descendant] = v
            else:
                if descendant_depend_info['opt_dep_group'] in var_groups.keys():
                    v = Bool(descendant)
                    var_groups[descendant_depend_info['opt_dep_group']].append(v)
                    var_mapping[descendant] = v
                else:
                    var_groups[descendant_depend_info['opt_dep_group']] = []
                    v = Bool(descendant)
                    var_groups[descendant_depend_info['opt_dep_group']].append(v)
                    var_mapping[descendant] = v

    for var_group in var_groups.keys():
        solver.add(Or(var_groups[var_group]))

    print(solver)

    models = get_models(solver, 100)

    print(models)

    for m in models:

        packages_to_install = []
        packages_to_uninstall = []

        G_copy = G.copy()

        c.execute("SELECT package_id FROM state")
        res = c.fetchall()
        state_ids = list(map(lambda x: x['package_id'], res))

        for node in G_copy.nodes(data=True):
            try:
                if not m[var_mapping[node[0]]]:
                    G.remove_node(node[0])
            except KeyError:
                pass

        cost = 0

        nodes = G.nodes(data=True)
        for n in nx.algorithms.dag.topological_sort(G.reverse()):
            if n not in install_order_ids and n not in state_ids:
                c.execute("SELECT name, version, weight FROM packages WHERE id = %s", [n])
                res = c.fetchone()
                install_order.append("+" + res['name'] + "=" + res['version'])
                install_order_ids.append(n)
                cost += res['weight']
            elif n in state_ids or n in install_order_ids:
                # Only uninstall if its in the state, or it's already been installed
                c.execute("SELECT name, version FROM packages WHERE id = %s", [n])
                res = c.fetchone()
                install_order.append("-" + res['name'] + "=" + res['version'])
                install_order_ids.append(n)
                cost += 10 ** 6

        sols.append(json.dumps(install_order))
        costs.append(cost)
    c.execute(unset_for_key_check)
    c.execute(del_everything_except_pkg)
    c.execute(set_for_key_check)
    conn.commit()

print(costs)
smallest_index = costs.index(min(costs))
print(sols[smallest_index])

c.execute(unset_for_key_check)
c.execute(del_pkg)
c.execute(set_for_key_check)
conn.commit()
