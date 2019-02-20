import argparse
import json
import networkx as nx
from operator import *
from packaging import version as vparser
import pymysql.cursors
import matplotlib.pyplot as plt
import sys

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


def parse_constraints(constraints):
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
                c.execute("SELECT id FROM packages WHERE name = %s ORDER BY version", [constraint[1:]])
                id = c.fetchone()
                installs.append(id['id'])
        else:
            if "=" in constraint:
                const = constraint[1:].split("=")
                c.execute("SELECT id FROM packages WHERE name = %s AND version = %s", [const[0], const[1]])
                id = c.fetchone()
                uninstalls.append(id['id'])
            else:
                c.execute("SELECT id FROM packages WHERE name = %s ORDER BY version", [constraint[1:]])
                id = c.fetchone()
                uninstalls.append(id['id'])

    return installs, uninstalls


def add_deps(pid):
    c.execute("SELECT depends FROM packages WHERE id = %s", [pid])
    depends = c.fetchone()
    depends = json.loads(depends['depends'])
    if len(depends) > 0:
        opt_dep_group = 0
        for dlist in depends:
            if len(dlist) == 1:
                must_be_installed = 1
            else:
                must_be_installed = 0
            for dep in dlist:
                package_name, package_version, package_req = parse_vstring(dep)
                if package_req is not None and package_version is not None:
                    c.execute("SELECT id, version FROM packages WHERE name = %s", [package_name])
                    packages = c.fetchall()
                    if len(packages) != 0:
                        packages_rightversion = filter(lambda x: package_req(vparser.parse(x['version']), vparser.parse(package_version)), packages)
                        l = list(packages_rightversion)
                        if len(l) > 0:
                            depid = sorted(l, key=lambda x: vparser.parse(x['version']))[0]['id']
                            try:
                                c.execute("INSERT INTO depends(package_id, depend_package_id, must_be_installed, opt_dep_group) VALUES (%s, %s, %s, %s)", [pid, depid, must_be_installed, opt_dep_group])
                            except pymysql.IntegrityError:
                                pass
                else:
                    c.execute("SELECT id, version FROM packages WHERE name = %s", [package_name])
                    packages = c.fetchall()
                    if len(packages) != 0:
                        # We didn't find ANY packages in the repo with this name! That means that we should probably just ignore this dependency is even a thing
                        depid = sorted(packages, key=lambda x: vparser.parse(x['version']))[0]['id']
                        try:
                            c.execute("INSERT INTO depends(package_id, depend_package_id, must_be_installed, opt_dep_group) VALUES (%s, %s, %s, %s)", [pid, depid, must_be_installed, opt_dep_group])
                        except pymysql.IntegrityError:
                            pass
            opt_dep_group += 1
    conn.commit()


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
                            c.execute("INSERT INTO conflicts(package_id, conflict_package_id) VALUES (%s, %s)", [pid, con['id']])
                        except pymysql.IntegrityError:
                            pass
            else:
                c.execute("SELECT id FROM packages WHERE name = %s", [package_name])
                cons = c.fetchall()
                for con in cons:
                    try:
                        c.execute("INSERT INTO conflicts(package_id, conflict_package_id) VALUES (%s, %s)", [pid, con['id']])
                    except pymysql.IntegrityError:
                        pass
    conn.commit()


def add_dep_to_installs(package_id):
    add_deps(package_id)
    add_conflicts(package_id)
    check_in = "AND package_id NOT IN (" + ", ".join(map(str, uninstalls)) + ")" if len(uninstalls) > 0 else ""
    c.execute("SELECT depend_package_id, opt_dep_group, weight FROM depends, packages WHERE package_id = %s AND packages.id = %s " + check_in + " ORDER BY weight ASC", [package_id, package_id])
    tmp = c.fetchall() # Only get ID
    dependencies = []
    if len(tmp) != 0:
        prev_opt_dep_group = None
        for d in tmp:
            if d['opt_dep_group'] != prev_opt_dep_group:
                G.add_edge(package_id, d['depend_package_id'])
                if d['depend_package_id'] not in installs and d['depend_package_id'] not in dependencies:
                    dependencies.append(d['depend_package_id'])
                prev_opt_dep_group = d['opt_dep_group']
    else:
        # We don't have any dependencies, don't need to add to graph, just install whenever
        installs_no_deps.append(package_id)
    installs.extend(dependencies)
    map(lambda x: add_dep_to_installs(x), dependencies)


def add_conflict_to_uninstalls(package_id):
    add_deps(package_id)
    add_conflicts(package_id)
    c.execute("SELECT conflict_package_id FROM conflicts WHERE package_id = %s", [package_id])
    tmp = c.fetchall()
    conflicts = []
    for con in tmp:
        if con['conflict_package_id'] not in conflicts:
            conflicts.append(con['conflict_package_id'])
    uninstalls.extend(conflicts)
    map(lambda x: add_conflict_to_uninstalls(x), conflicts)


conn = make_conn()

c = conn.cursor()
c.execute(no_sql_notes)
c.execute(unset_for_key_check)
c.execute(del_pkg)
c.execute(set_for_key_check)
c.execute(package_db)
c.execute(conflicts_db)
c.execute(depends_db)
c.execute(state_db)

conn.commit()

for p in repository:
    # Index repo packages by name and version
    c.execute("INSERT INTO packages(name, version, weight, depends, conflicts) VALUES (%s, %s, %s, %s, %s)", [p['name'], p['version'], p['size'], json.dumps(p['depends']) if 'depends' in p.keys() else "[]", json.dumps(p['conflicts']) if 'conflicts' in p.keys() else "[]"])

conn.commit()

G = nx.DiGraph()

installs, uninstalls = parse_constraints(constraints)
installs_no_deps = []

for i in initial:
    if "=" in i:
        name, version = i.split("=")
        c.execute("SELECT id FROM packages WHERE name = %s and version = %s", [name, version])
        res = c.fetchone()
        c.execute("INSERT INTO state(package_id) VALUES(%s)", [res['id']])
    else:
        c.execute("SELECT id, version FROM packages WHERE name = %s", [i])
        ps = c.fetchall()
        pid = sorted(ps, key=lambda x: version.parse(x['version']))[0]['id']
        c.execute("INSERT INTO state(package_id) VALUES(%s)", [pid])

conn.commit()

for i in installs:
    add_conflict_to_uninstalls(i)

for i in installs:
    add_dep_to_installs(i)
    add_conflict_to_uninstalls(i)

install_order = []

for n in set(uninstalls):
    c.execute("SELECT name, version FROM packages, state WHERE id = %s AND package_id = %s", [n, n])
    res = c.fetchone()
    if res:
        install_order.append("-" + res['name'] + "=" + res['version'])

G.remove_edges_from(G.selfloop_edges())

for n in nx.algorithms.dag.lexicographical_topological_sort(G.reverse()):
    # Check if we've already installed this package:
    c.execute("SELECT package_id FROM state WHERE package_id = %s", [n])
    res = c.fetchone()
    if not res and n not in uninstalls:
        c.execute("SELECT name, version FROM packages WHERE id = %s", [n])
        res = c.fetchone()
        install_order.append("+" + res['name'] + "=" + res['version'])

for n in installs_no_deps:
    c.execute("SELECT name, version FROM packages WHERE id = %s", [n])
    res = c.fetchone()
    if n not in list(G.nodes) and n not in uninstalls:
        install_order.append("+" + res['name'] + "=" + res['version'])

print(json.dumps(install_order))

c.execute(unset_for_key_check)
c.execute(del_pkg)
c.execute(set_for_key_check)
conn.commit()