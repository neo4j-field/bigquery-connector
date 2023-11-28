# Generated from Pattern.g4 by ANTLR 4.13.0
from antlr4 import *

if "." in __name__:
    from .PatternParser import PatternParser
else:
    from PatternParser import PatternParser


# This class defines a complete listener for a parse tree produced by PatternParser.
class PatternListener(ParseTreeListener):
    # Enter a parse tree produced by PatternParser#patterns.
    def enterPatterns(self, ctx: PatternParser.PatternsContext):
        pass

    # Exit a parse tree produced by PatternParser#patterns.
    def exitPatterns(self, ctx: PatternParser.PatternsContext):
        pass

    # Enter a parse tree produced by PatternParser#pattern.
    def enterPattern(self, ctx: PatternParser.PatternContext):
        pass

    # Exit a parse tree produced by PatternParser#pattern.
    def exitPattern(self, ctx: PatternParser.PatternContext):
        pass

    # Enter a parse tree produced by PatternParser#nodePattern.
    def enterNodePattern(self, ctx: PatternParser.NodePatternContext):
        pass

    # Exit a parse tree produced by PatternParser#nodePattern.
    def exitNodePattern(self, ctx: PatternParser.NodePatternContext):
        pass

    # Enter a parse tree produced by PatternParser#edgePattern.
    def enterEdgePattern(self, ctx: PatternParser.EdgePatternContext):
        pass

    # Exit a parse tree produced by PatternParser#edgePattern.
    def exitEdgePattern(self, ctx: PatternParser.EdgePatternContext):
        pass

    # Enter a parse tree produced by PatternParser#labelOrType.
    def enterLabelOrType(self, ctx: PatternParser.LabelOrTypeContext):
        pass

    # Exit a parse tree produced by PatternParser#labelOrType.
    def exitLabelOrType(self, ctx: PatternParser.LabelOrTypeContext):
        pass

    # Enter a parse tree produced by PatternParser#properties.
    def enterProperties(self, ctx: PatternParser.PropertiesContext):
        pass

    # Exit a parse tree produced by PatternParser#properties.
    def exitProperties(self, ctx: PatternParser.PropertiesContext):
        pass

    # Enter a parse tree produced by PatternParser#property.
    def enterProperty(self, ctx: PatternParser.PropertyContext):
        pass

    # Exit a parse tree produced by PatternParser#property.
    def exitProperty(self, ctx: PatternParser.PropertyContext):
        pass

    # Enter a parse tree produced by PatternParser#name.
    def enterName(self, ctx: PatternParser.NameContext):
        pass

    # Exit a parse tree produced by PatternParser#name.
    def exitName(self, ctx: PatternParser.NameContext):
        pass


del PatternParser
