import argparse
import json
import networkx as nx
import sqlite3
from operator import *
from packaging import version
import MySQLdb

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

conn = MySQLdb.connect(host="dragon.kent.ac.uk",    # your host, usually localhost
                     user="wf44",         # your username
                     passwd="lsar/az",  # your password
                     db="wf44")        # name of the data base

#conn = sqlite3.connect(':memory:') # create database in memory

package_db = \
'''
CREATE TABLE packages (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(2147483647),
    version VARCHAR(2147483647),
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
    PRIMARY KEY (package_id) REFERENCES packages(id)
)
"""

c = conn.cursor()

c.execute(package_db)
c.execute(conflicts_db)
c.execute(depends_db)

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
                c.execute("SELECT id FROM packages WHERE name = ? AND version = ?", [const[0], const[1]])
                id = c.fetchone()
                installs.append(id[0])
            else:
                c.execute("SELECT id FROM packages WHERE name = ? ORDER BY version", [constraint[1:]])
                id = c.fetchone()
                installs.append(id[0])
        else:
            if "=" in constraint:
                const = constraint[1:].split("=")
                c.execute("SELECT id FROM packages WHERE name = ? AND version = ?", [const[0], const[1]])
                id = c.fetchone()
                uninstalls.append(id[0])
            else:
                c.execute("SELECT id FROM packages WHERE name = ? ORDER BY version", [constraint[1:]])
                id = c.fetchone()
                uninstalls.append(id[0])

    return installs, uninstalls


for p in repository:
    # Index repo packages by name and version
    c.execute("INSERT INTO packages(name, version, size) VALUES (?, ?, ?)", [p['name'], p['version'], p['size']])

conn.commit()

counter2 = 0
for p in repository:
    c.execute("SELECT id FROM packages WHERE name = ? and version = ?", [p['name'], p['version']])
    id = c.fetchone()[0]
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
                    c.execute("SELECT * FROM packages WHERE name = ?", [package_name])
                    packages = c.fetchall()
                    if packages != []:
                        packages_rightversion = filter(lambda x: package_req(version.parse(x[2]), version.parse(package_version)), packages)
                        l = list(packages_rightversion)
                        if len(l) > 0:
                            depid = sorted(l, key=lambda x: version.parse(x[2]))[0][0]
                            try:
                                c.execute("INSERT INTO depends(package_id, depend_package_id, must_be_installed) VALUES (?, ?, ?)", [id, depid, must_be_installed])
                            except sqlite3.IntegrityError:
                                pass
                else:
                    c.execute("SELECT * FROM packages WHERE name = ?", [package_name])
                    packages = c.fetchall()
                    if packages != []:
                        # We didn't find ANY packages in the repo with this name! That means that we should probably just ignore this dependency is even a thing
                        depid = sorted(packages, key=lambda x: version.parse(x[2]))[0][0]
                        try:
                            c.execute("INSERT INTO depends(package_id, depend_package_id, must_be_installed) VALUES (?, ?, ?)", [id, depid, must_be_installed])
                        except sqlite3.IntegrityError:
                            pass
    conn.commit()
    if 'conflicts' in p.keys():
        for conflict in p['conflicts']:
            package_name, package_version, package_req = parse_vstring(conflict)
            if package_req is not None and package_version is not None:
                c.execute("SELECT * FROM packages WHERE name = ?", [package_name])
                cons = c.fetchall()
                for con in cons:
                    if package_req(version.parse(con[2]), version.parse(package_version)):
                        try:
                            c.execute("INSERT INTO conflicts(package_id, conflict_package_id) VALUES (?, ?)", [id, con[0]])
                        except sqlite3.IntegrityError:
                            pass
            else:
                c.execute("SELECT id FROM packages WHERE name = ?", [package_name])
                cons = c.fetchall()
                for con in cons:
                    try:
                        c.execute("INSERT INTO conflicts(package_id, conflict_package_id) VALUES (?, ?)", [id, con[0]])
                    except sqlite3.IntegrityError:
                        pass
    conn.commit()
    counter2 += 1
    print(str(counter2) + " of " + str(len(repository)))
conn.commit()

def add_dep_to_installs(package_id):
    c.execute("SELECT * FROM depends WHERE package_id = ?", [package_id])
    tmp = c.fetchall() # Only get ID
    dependencies = []
    for d in tmp:
        G.add_edge(package_id, d[1])
        dependencies.append(d[1])
    installs.extend(dependencies)
    map(lambda x: add_dep_to_installs(x), dependencies)

def add_conflict_to_uninstalls(package_id):
    c.execute("SELECT * FROM conflicts WHERE package_id = ?", [package_id])
    tmp = c.fetchall()
    conflicts = []
    for con in tmp:
        conflicts.append(con[1])
    installs.extend(conflicts)
    map(lambda x: add_dep_to_installs(x), conflicts)

G = nx.DiGraph()

installs, uninstalls = parse_constraints(constraints)

for i in installs:
    add_dep_to_installs(i)
    add_conflict_to_uninstalls(i)

install_order = []

for n in uninstalls:
    print(n)

for n in nx.algorithms.dag.lexicographical_topological_sort(G.reverse()):
    c.execute("SELECT name, version FROM packages WHERE id = ?", [n])
    res = c.fetchone()
    install_order.append("+" + res[0] + "=" + res[1])

print(json.dumps(install_order))

