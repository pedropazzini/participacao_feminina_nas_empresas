
class Node:
    def __init__(self, key, value=None):
        self.key = key
        self.value = value
        self.childs = {}

    def add(self, node):
        if node.key not in self.childs:
            self.childs[node.key] = node

    def get_child(self, key):
        return self.childs.get(key)

class Tree:
    def __init__(self):
        self.node = Node('*')

    def insert(self, string, value):
        size = len(string)
        last_node = self.node
        for char in string[:-1]:
            child_node = last_node.get_child(char)
            if child_node is None:
                new_node = Node(char, None)
                last_node.add(new_node)
                last_node = new_node
            else:
                last_node = child_node
        last_node.add(Node(string[-1], value))

    def query(self, string):
        current_node = self.node
        size_string = len(string)
        for i, char in enumerate(string):
            current_node = current_node.get_child(char)
            if current_node is None and i != size_string - 1:
                return None
        return current_node.value if current_node is not None else None

