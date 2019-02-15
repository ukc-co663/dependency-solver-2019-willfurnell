import argparse
import json
import networkx as nx
from operator import *
from packaging import version
import pymysql.cursors
import matplotlib.pyplot as plt

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

# Connect to the database
conn = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='depsolve',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

#conn = sqlite3.connect(':memory:') # create database in memory

package_db = \
'''
CREATE TABLE packages (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    version VARCHAR(255),
    size INTEGER
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
no_sql_notes = "SET sql_notes = 0"
set_for_key_check = "SET foreign_key_checks = 1"
del_pkg = "DROP TABLE IF EXISTS packages, conflicts, depends, state"

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


for p in repository:
    # Index repo packages by name and version
    c.execute("INSERT INTO packages(name, version, size) VALUES (%s, %s, %s)", [p['name'], p['version'], p['size']])

conn.commit()

counter2 = 0
for p in repository:
    c.execute("SELECT id FROM packages WHERE name = %s and version = %s", [p['name'], p['version']])
    pid = c.fetchone()['id']
    if 'depends' in p.keys():
        for dlist in p['depends']:
            if len(dlist) == 1:
                must_be_installed = 1
            else:
                must_be_installed = 0
            for dep in dlist:
                #print(dep)
                #print(parse_vstring(dep))
                package_name, package_version, package_req = parse_vstring(dep)
                if package_req is not None and package_version is not None:
                    c.execute("SELECT id, version FROM packages WHERE name = %s", [package_name])
                    packages = c.fetchall()
                    if len(packages) != 0:
                        packages_rightversion = filter(lambda x: package_req(version.parse(x['version']), version.parse(package_version)), packages)
                        l = list(packages_rightversion)
                        if len(l) > 0:
                            depid = sorted(l, key=lambda x: version.parse(x['version']))[0]['id']
                            try:
                                c.execute("INSERT INTO depends(package_id, depend_package_id, must_be_installed) VALUES (%s, %s, %s)", [pid, depid, must_be_installed])
                            except pymysql.IntegrityError:
                                pass
                else:
                    c.execute("SELECT id, version FROM packages WHERE name = %s", [package_name])
                    packages = c.fetchall()
                    if len(packages) != 0:
                        # We didn't find ANY packages in the repo with this name! That means that we should probably just ignore this dependency is even a thing
                        depid = sorted(packages, key=lambda x: version.parse(x['version']))[0]['id']
                        try:
                            c.execute("INSERT INTO depends(package_id, depend_package_id, must_be_installed) VALUES (%s, %s, %s)", [pid, depid, must_be_installed])
                        except pymysql.IntegrityError:
                            pass
    conn.commit()
    if 'conflicts' in p.keys():
        for conflict in p['conflicts']:
            package_name, package_version, package_req = parse_vstring(conflict)
            if package_req is not None and package_version is not None:
                c.execute("SELECT id, version FROM packages WHERE name = %s", [package_name])
                cons = c.fetchall()
                for con in cons:
                    if package_req(version.parse(con['version']), version.parse(package_version)):
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
    counter2 += 1
    print(str(counter2) + " of " + str(len(repository)))
conn.commit()


def add_dep_to_installs(package_id):
    #TODO: We can get circular dependencies here
    # At this stage we need to check for optional dependencies, and if we have a circular graph, then remove one of the
    # optional dependencies, to get rid of the circle

    # ALSO IF NO DEPENDENCIES, STILL ADD TO INSTALLS!
    c.execute("SELECT depend_package_id FROM depends WHERE package_id = %s", [package_id])
    tmp = c.fetchall() # Only get ID
    dependencies = []
    if len(tmp) != 0:
        for d in tmp:
            G.add_edge(package_id, d['depend_package_id'])
            if d['depend_package_id'] not in installs:
                dependencies.append(d['depend_package_id'])
    else:
        # We don't have any dependencies, don't need to add to graph, just install whenever
        installs_no_deps.append(package_id)
    installs.extend(dependencies)
    map(lambda x: add_dep_to_installs(x), dependencies)


def add_conflict_to_uninstalls(package_id):
    c.execute("SELECT conflict_package_id FROM conflicts WHERE package_id = %s", [package_id])
    tmp = c.fetchall()
    conflicts = []
    for con in tmp:
        if con['conflict_package_id'] not in conflicts:
            conflicts.append(con['conflict_package_id'])
    uninstalls.extend(conflicts)
    map(lambda x: add_conflict_to_uninstalls(x), conflicts)

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

for i in installs:
    add_dep_to_installs(i)
    add_conflict_to_uninstalls(i)

install_order = []

for n in uninstalls:
    c.execute("SELECT name, version FROM packages, state WHERE id = %s AND package_id = %s", [n, n])
    res = c.fetchone()
    if res:
        install_order.append("-" + res['name'] + "=" + res['version'])

for n in nx.algorithms.dag.lexicographical_topological_sort(G.reverse()):
    c.execute("SELECT name, version FROM packages WHERE id = %s", [n])
    res = c.fetchone()
    install_order.append("+" + res['name'] + "=" + res['version'])

for n in installs_no_deps:
    c.execute("SELECT name, version FROM packages WHERE id = %s", [n])
    res = c.fetchone()
    install_order.append("+" + res['name'] + "=" + res['version'])

print(json.dumps(install_order))

c.execute(unset_for_key_check)
c.execute(del_pkg)
c.execute(set_for_key_check)
conn.commit()