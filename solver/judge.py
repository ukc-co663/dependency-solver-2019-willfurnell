#!/usr/bin/env python3

"""
Radu Grigore's judge.py script from here: https://github.com/ukc-co663/depsolver/blob/master/tests/judge.py
Slightly modified to work with Will Furnell's solution.
"""

from collections import defaultdict
from functools import total_ordering
import re
import sys

commands = None
final_constraints = None
state = None
repository = None

class BadStateException(Exception):
  pass

@total_ordering
class Version:
  def __init__(self, verstr):
    if not re.match('[0-9]+(.[0-9]+)*', verstr):
      print("bad v")
      raise BadStateException
    self.nums = tuple(int(x) for x in verstr.split('.'))

  def __hash__(self):
    return hash(self.nums)

  def __eq__(self, other):
    return self.nums == other.nums

  def __lt__(self, other):
    return self.nums < other.nums

  def __repr__(self):
    return '.'.join(str(x) for x in self.nums)

class Package:
  def __init__(self, reference):
    if not re.match('[.+a-zA-Z0-9-]+=[.0-9]+', reference):
      print("bad p")
      raise BadStateException
    [n, v] = reference.split('=')
    self.name = n
    self.version = Version(v)

  def __hash__(self):
    return 31 * hash(self.name) + hash(self.version)

  def __eq__(self, other):
    return self.name == other.name and self.version == other.version

  def __repr__(self):
    return '{}={}'.format(self.name, self.version)

class Command:
  def __init__(self, cmdstr):
    if not re.match('[+-].*', cmdstr):
      print("bad command")
      raise BadStateException
    self.action = cmdstr[0]
    self.package = Package(cmdstr[1:])

  def __repr__(self):
    return '{}{}'.format(self.action, self.package)

class PackageRange:
  def __init__(self, rangestr):
    m = re.match('([.+a-zA-Z0-9-]+)((=|<|>|<=|>=)([.0-9]+))?', rangestr)
    if not m:
      print("bad range")
      raise BadStateException
    self.name = m.group(1)
    self.minimum = None
    self.maximum = None
    self.inclusive = False
    if m.group(2):
      self.inclusive = ('=' in m.group(3))
      v = Version(m.group(4))
      if '<' in m.group(3):
        self.maximum = v
      elif '>' in m.group(3):
        self.minimum = v
      else:
        self.minimum = self.maximum = v

  def __repr__(self):
    r = self.name
    if self.minimum and not self.maximum:
      r += '>'
    if self.maximum and not self.minimum:
      r += '<'
    if self.inclusive:
      r += '='
    if self.minimum:
      r += str(self.minimum)
    elif self.maximum:
      r += str(self.maximum)
    return r

  def __hash__(self):
    h = 0
    h = 31 * h + hash(self.name)
    h = 31 * h + hash(self.minimum)
    h = 31 * h + hash(self.maximum)
    h = 31 * h + hash(self.inclusive)
    return h

  def __eq__(self, other):
    ok = True
    ok = ok and self.name == other.name
    ok = ok and self.minimum == other.minimum
    ok = ok and self.maximum == other.maximum
    ok = ok and self.inclusive == other.inclusive
    return ok

  def has(self, package):
    if self.name != package.name:
      return False
    if self.minimum:
      if self.inclusive:
        if not (self.minimum <= package.version):
          return False
      else:
        if not (self.minimum < package.version):
          return False
    if self.maximum:
      if self.inclusive:
        if not (package.version <= self.maximum):
          return False
      else:
        if not (package.version < self.maximum):
          return False
    return True

class PackageProperties:
  def __init__(self, depends, conflicts, size):
    self.depends = depends
    self.conflicts = conflicts
    self.size = size

class Constraint:
  def __init__(self, constraintstr):
    if not re.match('[-+].*', constraintstr):
      print("bad constraint")
      raise BadStateException
    self.kind = constraintstr[0]
    self.packageRange = PackageRange(constraintstr[1:])

  def __repr__(self):
    return '{}{}'.format(self.kind, self.packageRange)

def load_commands(data):
  global commands
  commands = []
  for s in data:
    commands.append(Command(s))

def load_constraints(data):
  global final_constraints
  final_constraints = set()
  for s in data:
    final_constraints.add(Constraint(s))

def load_state(data):
  global state
  state = set()
  for s in data:
    state.add(Package(s))

def load_repository(data):
  global repository
  repository = {}
  for p in data:
    package = Package('{}={}'.format(p['name'], p['version']))
    if package in repository:
      return False
    depends = []
    pdep = p['depends'] if 'depends' in p else []
    for clause in pdep:
      depends.append([PackageRange(r) for r in clause])
    pconf = p['conflicts'] if 'conflicts' in p else []
    conflicts = [PackageRange(r) for r in pconf]
    size = int(p['size'])
    repository[package] = PackageProperties(depends, conflicts, size)

def load_all(commands, constraints, state, repository):
  load_commands(commands)
  load_constraints(constraints)
  load_state(state)
  load_repository(repository)

clauses = None
repo_clauses_count = None
occurrences = None
packages = None
rpackages = None
watches = None  # watches[c] is some literal which makes the clause c true (could be None)
unsat_clauses = None
val = None # val[v] in [v, -v]

class Unsat(Exception):
  def __init__(self, ps):
    self.clause = ps
  def __str__(self):
    return ' '.join(self.s(p) for p in self.clause)
  def s(self, p):
    return ('+' if p > 0 else '-') + str(packages[abs(p)])

def find_watch(ps):
  global val
  for p in ps:
    if val[abs(p)] == p:
      return p
  return None

def set_watch(c):
  global clauses
  global watches
  global unsat_clauses
  if watches[c] is None:
    unsat_clauses.remove(c)
  watches[c] = find_watch(clauses[c])
  if watches[c] is None:
    unsat_clauses.add(c)

def add_clause(one_clause):
  global clauses
  global watches
  n = len(clauses)
  for l in one_clause:
    occurrences[l].add(n)
  clauses.append(one_clause)
  watches.append(None)
  unsat_clauses.add(n)
  set_watch(n)

def preprocess():
  global clauses
  global occurrences
  global packages
  global repo_clauses_count
  global repository
  global rpackages
  global unsat_clauses
  global val
  global watches

  # We'll refer to packages by positive integers.
  packages = [None] + list(repository.keys())
  rpackages = { packages[i] : i for i in range(1, len(packages)) }
  val = [None] + [i if packages[i] in state else -i for i in range(1,len(packages))]

  # For each PackageRange in the repo, compute which packages it matches.
  pranges = set()
  for props in repository.values():
    for clause in props.depends:
      for constraint in clause:
        pranges.add(constraint)
    for r in props.conflicts:
      pranges.add(r)
  for constraint in final_constraints:
    pranges.add(constraint.packageRange)
  versions = defaultdict(list)
  for package in repository.keys():
    versions[package.name].append(package.version)
  inrange = {} # lists all packages in a given range, by their indices
  for r in pranges:
    inrange[r] = []
    for v in versions[r.name]:
      p = Package('{}={}'.format(r.name, v))
      if r.has(p):
        inrange[r].append(rpackages[p])

  # Add clauses for depends and conflicts.
  clauses = []
  watches = []
  unsat_clauses = set()
  occurrences = defaultdict(set)
  for package, props in repository.items():
    p = rpackages[package]
    for dclause in props.depends:
      new_clause = [-p]
      for r in dclause:
        for q in inrange[r]:
          new_clause.append(q)
      add_clause(new_clause)
    for r in props.conflicts:
      for q in inrange[r]:
        add_clause([-p, -q])
  repo_clauses_count = len(clauses)

  # Add clauses for final_constraints.
  for constraint in final_constraints:
    if constraint.kind == '-':
      for q in inrange[constraint.packageRange]:
        add_clause([-q])
    else:
      add_clause(list(inrange[constraint.packageRange]))

def set_literal(p):
  if val[abs(p)] == p:
    return
  val[abs(p)] = p
  if p < 0:
    state.remove(packages[-p])
  else:
    state.add(packages[p])
  for c in occurrences[-p]:
    if watches[c] == -p:
      set_watch(c)
  for c in occurrences[p]:
    if watches[c] is None:
      watches[c] = p
      unsat_clauses.remove(c)

def flip_var(v):
  set_literal(-val[v])

def install_package(package):
  if package in state:
    return False
  if package not in repository:
    return False
  set_literal(rpackages[package])

def uninstall_package(package):
  if package not in state:
    return False
  set_literal(-rpackages[package])


def is_valid_state(command_json, constraints_json, state_json, repository_json):
  global commands
  global final_constraints
  global repository
  load_all(command_json, constraints_json, state_json, repository_json)
  preprocess()
  if any(c < repo_clauses_count for c in unsat_clauses):
    return False
  for cmd in commands:
    if cmd.action == '+':
      install_package(cmd.package)
    else:
      uninstall_package(cmd.package)
    for c in unsat_clauses:
      if c < repo_clauses_count:
       return False
  if unsat_clauses:
    c = unsat_clauses.pop()
    return False
  return True