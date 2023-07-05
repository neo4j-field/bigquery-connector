import typing

from antlr4 import *

from .PatternLexer import PatternLexer
from .PatternListener import PatternListener
from .PatternParser import PatternParser


class Pattern:
    def type(self):
        raise NotImplemented()

    @staticmethod
    def is_node(p: typing.Any) -> bool:
        return isinstance(p, NodePattern)

    @staticmethod
    def is_edge(p: typing.Any) -> bool:
        return isinstance(p, EdgePattern)


class NodePattern(Pattern):
    def __init__(self):
        self.label: str = ""
        self.properties: list[str] = []

    def type(self):
        return "node"

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"(:{self.label} {{{self.properties}}})"


class EdgePattern(Pattern):
    def __init__(self):
        self.type: str = ""
        self.properties: list[str] = []

    def type(self):
        return "edge"

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"[:{self.type}{{{self.properties}}}]"


class PatternVisitor(PatternListener):
    def __init__(self, result: list = []):
        self._pattern = None
        self.result = result

    def enterNodePattern(self, ctx: PatternParser.NodePatternContext):
        self._pattern = NodePattern()

    def exitNodePattern(self, ctx: PatternParser.NodePatternContext):
        self.result.append(self._pattern)
        self._pattern = None

    def enterEdgePattern(self, ctx: PatternParser.EdgePatternContext):
        self._pattern = EdgePattern()

    def exitEdgePattern(self, ctx: PatternParser.EdgePatternContext):
        self.result.append(self._pattern)
        self._pattern = None

    def enterLabelOrType(self, ctx: PatternParser.LabelOrTypeContext):
        label_or_type = ctx.getText()[1:]
        if isinstance(self._pattern, NodePattern):
            self._pattern.label = label_or_type
        else:
            self._pattern.type = label_or_type

    def enterProperty(self, ctx: PatternParser.PropertyContext):
        self._pattern.properties.append(ctx.getText())


def parse_pattern(pattern_input: str) -> list[Pattern]:
    lexer = PatternLexer(InputStream(pattern_input))
    stream = CommonTokenStream(lexer)
    parser = PatternParser(stream)
    tree = parser.patterns()

    patterns: list[Pattern] = []
    listener = PatternVisitor(patterns)
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    return patterns
