import typing

from antlr4 import InputStream, CommonTokenStream, ParseTreeWalker

from .PatternLexer import PatternLexer
from .PatternListener import PatternListener
from .PatternParser import PatternParser


class Pattern:
    def pattern_type(self) -> str:
        raise NotImplementedError()

    @staticmethod
    def is_node(p: typing.Any) -> bool:
        return isinstance(p, NodePattern)

    @staticmethod
    def is_edge(p: typing.Any) -> bool:
        return isinstance(p, EdgePattern)


class NodePattern(Pattern):
    def __init__(self) -> None:
        self.label: str = ""
        self.properties: list[str] = []

    def pattern_type(self) -> str:
        return "node"

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return f"(:{self.label} {{{self.properties}}})"


class EdgePattern(Pattern):
    def __init__(self) -> None:
        self.type: str = ""
        self.properties: list[str] = []

    def pattern_type(self) -> str:
        return "edge"

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return f"[:{self.type}{{{self.properties}}}]"


class PatternVisitor(PatternListener):
    _pattern: typing.Optional[Pattern] = None

    def __init__(self, result: list[Pattern] = []) -> None:
        self.result = result

    def enterNodePattern(self, ctx: PatternParser.NodePatternContext) -> None:
        self._pattern = NodePattern()

    def exitNodePattern(self, ctx: PatternParser.NodePatternContext) -> None:
        if not self._pattern:
            raise ValueError("expected a pattern but got None")

        self.result.append(self._pattern)
        self._pattern = None

    def enterEdgePattern(self, ctx: PatternParser.EdgePatternContext) -> None:
        self._pattern = EdgePattern()

    def exitEdgePattern(self, ctx: PatternParser.EdgePatternContext) -> None:
        if not self._pattern:
            raise ValueError("expected a pattern but got None")

        self.result.append(self._pattern)
        self._pattern = None

    def enterLabelOrType(self, ctx: PatternParser.LabelOrTypeContext) -> None:
        label_or_type = ctx.getText()[1:]
        if isinstance(self._pattern, NodePattern):
            self._pattern.label = label_or_type
        elif isinstance(self._pattern, EdgePattern):
            self._pattern.type = label_or_type
        else:
            raise ValueError(f"unexpected pattern type: ${type(self._pattern)}")

    def enterProperty(self, ctx: PatternParser.PropertyContext) -> None:
        if isinstance(self._pattern, NodePattern):
            self._pattern.properties.append(ctx.getText())
        elif isinstance(self._pattern, EdgePattern):
            self._pattern.properties.append(ctx.getText())
        else:
            raise ValueError(f"unexpected pattern type: ${type(self._pattern)}")


def parse_pattern(pattern_input: str) -> list[Pattern]:
    lexer = PatternLexer(InputStream(pattern_input))
    stream = CommonTokenStream(lexer)
    parser = PatternParser(stream)
    tree = parser.patterns()  # type: ignore [no-untyped-call]

    patterns: list[Pattern] = []
    listener = PatternVisitor(patterns)
    walker = ParseTreeWalker()
    walker.walk(listener, tree)

    return patterns
