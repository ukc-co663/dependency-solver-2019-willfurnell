import argparse
import json
#from .package import Package
import networkx as nx
import matplotlib.pyplot as plt
import sqlite3

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

conn = sqlite3.connect(':memory:') # create database in memory

package_db = \
'''
CREATE TABLE packages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    PRIMARY KEY (package_id, depend_package_id),
    FOREIGN KEY (package_id) REFERENCES packages(id),
    FOREIGN KEY (depend_package_id) REFERENCES packages(id)
);
"""

c = conn.cursor()

c.execute(package_db)
c.execute(conflicts_db)
c.execute(depends_db)

conn.commit()


def parse_vstring(version_string):
    if ">=" in version_string:
        return (version_string.split(">=")[0], version_string.split(">=")[1], ">=")
    elif "<=" in version_string:
        return (version_string.split("<=")[0], version_string.split("<=")[1], "<=")
    elif "=" in version_string:
        return (version_string.split("=")[0], version_string.split("=")[1], "=")
    else:
        return version_string, None, None


def parse_constraints(constraints):
    installs = []
    uninstalls = []
    for constraint in constraints:
        if constraint[0] == "+":
            if "=" in constraint:
                c = constraint[1:].split("=")
                installs.append((c[0], c[1]))
            else:
                installs.append((c[0], "latest"))
        else:
            if "=" in constraint:
                c = constraint[1:].split("=")
                uninstalls.append((c[0], c[1]))
            else:
                uninstalls.append((c[0], "latest"))

    return installs, uninstalls


for p in repository:
    # Index repo packages by name and version
    c.execute("INSERT INTO packages(name, version, size) VALUES (?, ?, ?)", [p['name'], p['version'], p['size']])

conn.commit()

c.execute("SELECT * FROM packages")

print(c.fetchall())

for p in repository:
    c.execute("SELECT id FROM packages WHERE name = ? and version = ?", [p['name'], p['version']])
    id = c.fetchone()[0]

    if 'depends' in p.keys():
        for dlist in p['depends']:
            for dep in dlist:
                print(parse_vstring(dep))
                package_name, package_version, package_req = parse_vstring(dep)
                if package_req is not None and package_version is not None:
                    c.execute("SELECT id FROM packages WHERE version " + package_req + " ? AND name = ?", [package_version, package_name])
                    depid = c.fetchone()[0]
                    c.execute("INSERT INTO depends(package_id, depend_package_id) VALUES (?, ?)", [id, depid])
                else:
                    c.execute("SELECT id FROM packages WHERE name = ? ORDER BY version", [package_name])
                    depid = c.fetchone()[0]
                    c.execute("INSERT INTO depends(package_id, depend_package_id) VALUES (?, ?)", [id, depid])

    if 'conflicts' in p.keys():
        for conflict in p['conflicts']:
            package_name, package_version, package_req = parse_vstring(conflict)
            if package_req is not None and package_version is not None:
                c.execute("SELECT id FROM packages WHERE version " + package_req + " ? AND name = ?", [package_version, package_name])
                cons = c.fetchall()
                print(cons)
                for con in cons:
                    c.execute("INSERT INTO conflicts(package_id, conflict_package_id) VALUES (?, ?)", [id, con[0]])
            else:
                c.execute("SELECT id FROM packages WHERE name = ? ORDER BY version", [package_name])
                cons = c.fetchall()
                for con in cons:
                    c.execute("INSERT INTO conflicts(package_id, conflict_package_id) VALUES (?, ?)", [id, con[0]])

conn.commit()

G = nx.DiGraph()

installs, uninstalls = parse_constraints(constraints)

#for install in installs:

nx.algorithms.dag.lexicographical_topological_sort(G)

nx.draw(G, with_labels=True)
plt.draw()
plt.show()

