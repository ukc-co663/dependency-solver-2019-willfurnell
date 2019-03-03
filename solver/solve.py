import argparse
import json
import networkx as nx
from operator import *
from packaging import version as vparser
import pymysql.cursors
import matplotlib.pyplot as plt
import sys
from z3 import Solver, Bool, Not, Or, And, unsat, unknown, Z3Exception

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

opt_dep_group = 0


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
                c.execute("SELECT id FROM packages WHERE name = %s ORDER BY weight", [constraint[1:]])
                id = c.fetchone()
                installs.append(id['id'])
        else:
            if "=" in constraint:
                const = constraint[1:].split("=")
                c.execute("SELECT id FROM packages WHERE name = %s AND version = %s", [const[0], const[1]])
                id = c.fetchone()
                uninstalls.append(id['id'])
            else:
                c.execute("SELECT id FROM packages WHERE name = %s ORDER BY weight", [constraint[1:]])
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


def add_deps(pid):
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
    if package_id not in seen:
        seen.append(package_id)
        add_deps(package_id)
        add_conflicts(package_id)
        check_in = "AND package_id NOT IN (" + ", ".join(map(str, uninstalls)) + ")" if len(uninstalls) > 0 else ""
        #check_in_2 = "AND depend_package_id NOT IN (" + ", ".join(map(str, installs)) + ")" if len(installs) > 0 else ""
        check_in_2 = ""
        c.execute("SELECT depend_package_id, opt_dep_group, weight, must_be_installed, weight FROM depends, packages WHERE package_id = %s AND packages.id = %s " + check_in + check_in_2 + " ORDER BY weight ASC", [package_id, package_id])
        tmp = c.fetchall() # Only get ID
        dependencies = []
        if len(tmp) != 0:
            for d in tmp:
                add_deps(d['depend_package_id'])
                add_conflicts(d['depend_package_id'])
                add_conflict_to_uninstalls(d['depend_package_id'])
                if d['depend_package_id'] not in installs:
                    add_dep_to_installs(d['depend_package_id'])
                ii, _ = parse_constraints(constraints)
                #if d['depend_package_id'] not in ii:
                G.add_node(d['depend_package_id'], opt_dep_group=d['opt_dep_group'], required=0,
                           weight=d['weight'], conflict=False)
                G.add_edge(package_id, d['depend_package_id'])
                if d['depend_package_id'] not in dependencies:
                    dependencies.append(d['depend_package_id'])
        else:
            # THIS NEEDS TO BE CHANGED! Just because we don't have any dependencies doesn't mean we are required!
            add_conflict_to_uninstalls(package_id)
            # We don't have any dependencies, don't need to add to graph, just install whenever
            #ii, _ = parse_constraints(constraints) # For some stupid reason initial_installs was being overwritten when this was being called - no idea why!
            #if package_id in ii:
            #    G.add_node(package_id, required=1, opt_dep_group=-1, conflict=False)
            #else:
            ii, _ = parse_constraints(constraints)
            if package_id not in ii:
                G.add_node(package_id, required=0, opt_dep_group=-1, conflict=False)
            installs_no_deps.append(package_id)
        installs.extend(dependencies)


def add_conflict_to_uninstalls(package_id):
    add_conflicts(package_id)
    c.execute("SELECT conflict_package_id FROM conflicts WHERE package_id = %s", [package_id])
    tmp = c.fetchall()
    conflicts = []
    for con in tmp:
        ii, _ = parse_constraints(constraints)
        if con['conflict_package_id'] not in ii:
            G.add_node(con['conflict_package_id'], conflict=True)
            all_conflicts.append(con['conflict_package_id'])
        G.add_edge(package_id, con['conflict_package_id'])
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

c.execute("SELECT id, name FROM packages")
ps = c.fetchall()
#print(ps)

G = nx.DiGraph()

installs, uninstalls = parse_constraints(constraints)
initial_installs = installs
initial_uninstalls = uninstalls
installs_no_deps = []
install_order = []
install_order_ids = []
state = []
all_conflicts = []
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
        c.execute("SELECT id, weight FROM packages WHERE name = %s ORDER BY weight ASC", [i])
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
ii, _ = parse_constraints(constraints)
for i in ii:
    #print("Install: " + str(i))
    G.add_node(i, required=1, opt_dep_group=-1, conflict=False)
    add_dep_to_installs(i)

solver = Solver()

var_mapping = {}
node_groups =[]
trues = []
ands = []


# Pseudocode

# Go through graph in reverse order
# Get all the direct descendants of a node
# These must be conflicts or dependencies
# So And([list of direct descendants])
# Inside the And we also may have Or, which would be the optional dependency groups
# Then get the direct descendents of these nodes etc. etc.
# Eventually we will have translated the whole graph structure to a SAT problem, can solve this and get what we need
# to install

#for root_nodes in

cycles = nx.recursive_simple_cycles(G)
for cycle in cycles:
    G.remove_nodes_from(cycle[1:])

for n in G.nodes(data=True):
    direct_descendants = G[n[0]].keys()
    nodes = G.nodes(data=True)
    node_descendant = []
    var_groups = {}
    for descendant in direct_descendants:
        if 'conflict' in nodes[descendant].keys() and nodes[descendant]['conflict'] is True:
            v = Bool(descendant)
            node_descendant.append(Not(v))
            var_mapping[descendant] = v
        elif 'required' in nodes[descendant].keys() and nodes[descendant]['required'] == 1:
            node_descendant.append(True)
            trues.append(descendant)
        else:
            #print("got here")
            if nodes[descendant]['opt_dep_group'] in var_groups.keys():
                v = Bool(descendant)
                var_groups[nodes[descendant]['opt_dep_group']].append(v)
                var_mapping[descendant] = v
            else:
                var_groups[nodes[descendant]['opt_dep_group']] = []
                v = Bool(descendant)
                var_groups[nodes[descendant]['opt_dep_group']].append(v)
                var_mapping[descendant] = v

    ors = []
    for var_group in var_groups:
        ors.append(Or(var_groups[var_group]))
    if ors:
        node_descendant.append(And(ors))
    if node_descendant:
        node_groups.append(And(node_descendant))

solver.add(And(node_groups))

#print(all_conflicts)

#nx.draw(G, with_labels=True)
#plt.show()

#print(solver)

r = solver.check()

if r == unsat:
    print("no solution")
    exit(0)
elif r == unknown:
    print("failed to solve")
    try:
        print(solver.model())
    except Z3Exception:
        pass
    exit(0)

m = solver.model()
#print(m)

packages_to_install = []
packages_to_uninstall = []

packages_to_install.extend(trues)

G_copy = G.copy()

c.execute("SELECT package_id FROM state")
res = c.fetchall()
state_ids = list(map(lambda x: x['package_id'], res))

for node in G_copy.nodes(data=True):
    try:
        if node[0] not in trues and not m[var_mapping[node[0]]] and not node[0] in state_ids:
            G.remove_node(node[0])
    except KeyError:
        pass

#nx.draw(G, with_labels=True)
#plt.show()

for n in nx.algorithms.dag.topological_sort(G.reverse()):
    if n not in all_conflicts and n not in install_order_ids:
        c.execute("SELECT name, version FROM packages WHERE id = %s", [n])
        res = c.fetchone()
        install_order.append("+" + res['name'] + "=" + res['version'])
        install_order_ids.append(n)
    elif n in state_ids or n in install_order_ids:
        # Only uninstall if its in the state, or it's already been installed
        c.execute("SELECT name, version FROM packages WHERE id = %s", [n])
        res = c.fetchone()
        install_order.append("-" + res['name'] + "=" + res['version'])
        install_order_ids.append(n)

print(json.dumps(install_order))


c.execute(unset_for_key_check)
c.execute(del_pkg)
c.execute(set_for_key_check)
conn.commit()