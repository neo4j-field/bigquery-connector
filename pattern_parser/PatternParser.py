# Generated from Pattern.g4 by ANTLR 4.13.0
# encoding: utf-8
from antlr4 import *
from io import StringIO
import sys
if sys.version_info[1] > 5:
	from typing import TextIO
else:
	from typing.io import TextIO

def serializedATN():
    return [
        4,1,11,65,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,
        6,2,7,7,7,1,0,1,0,1,0,5,0,20,8,0,10,0,12,0,23,9,0,1,0,1,0,1,1,1,
        1,3,1,29,8,1,1,2,1,2,1,2,3,2,34,8,2,1,2,1,2,1,3,1,3,1,3,3,3,41,8,
        3,1,3,1,3,1,4,1,4,1,4,1,5,1,5,1,5,1,5,5,5,52,8,5,10,5,12,5,55,9,
        5,3,5,57,8,5,1,5,1,5,1,6,1,6,1,7,1,7,1,7,0,0,8,0,2,4,6,8,10,12,14,
        0,1,2,0,9,9,11,11,62,0,16,1,0,0,0,2,28,1,0,0,0,4,30,1,0,0,0,6,37,
        1,0,0,0,8,44,1,0,0,0,10,47,1,0,0,0,12,60,1,0,0,0,14,62,1,0,0,0,16,
        21,3,2,1,0,17,18,5,1,0,0,18,20,3,2,1,0,19,17,1,0,0,0,20,23,1,0,0,
        0,21,19,1,0,0,0,21,22,1,0,0,0,22,24,1,0,0,0,23,21,1,0,0,0,24,25,
        5,0,0,1,25,1,1,0,0,0,26,29,3,4,2,0,27,29,3,6,3,0,28,26,1,0,0,0,28,
        27,1,0,0,0,29,3,1,0,0,0,30,31,5,2,0,0,31,33,3,8,4,0,32,34,3,10,5,
        0,33,32,1,0,0,0,33,34,1,0,0,0,34,35,1,0,0,0,35,36,5,3,0,0,36,5,1,
        0,0,0,37,38,5,6,0,0,38,40,3,8,4,0,39,41,3,10,5,0,40,39,1,0,0,0,40,
        41,1,0,0,0,41,42,1,0,0,0,42,43,5,7,0,0,43,7,1,0,0,0,44,45,5,8,0,
        0,45,46,3,14,7,0,46,9,1,0,0,0,47,56,5,4,0,0,48,53,3,12,6,0,49,50,
        5,1,0,0,50,52,3,12,6,0,51,49,1,0,0,0,52,55,1,0,0,0,53,51,1,0,0,0,
        53,54,1,0,0,0,54,57,1,0,0,0,55,53,1,0,0,0,56,48,1,0,0,0,56,57,1,
        0,0,0,57,58,1,0,0,0,58,59,5,5,0,0,59,11,1,0,0,0,60,61,3,14,7,0,61,
        13,1,0,0,0,62,63,7,0,0,0,63,15,1,0,0,0,6,21,28,33,40,53,56
    ]

class PatternParser ( Parser ):

    grammarFileName = "Pattern.g4"

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    sharedContextCache = PredictionContextCache()

    literalNames = [ "<INVALID>", "','", "'('", "')'", "'{'", "'}'", "'['", 
                     "']'", "':'" ]

    symbolicNames = [ "<INVALID>", "COMMA", "LPAREN", "RPAREN", "LBRACE", 
                      "RBRACE", "LSQUARE", "RSQUARE", "COLON", "ID", "WS", 
                      "ESC_LITERAL" ]

    RULE_patterns = 0
    RULE_pattern = 1
    RULE_nodePattern = 2
    RULE_edgePattern = 3
    RULE_labelOrType = 4
    RULE_properties = 5
    RULE_property = 6
    RULE_name = 7

    ruleNames =  [ "patterns", "pattern", "nodePattern", "edgePattern", 
                   "labelOrType", "properties", "property", "name" ]

    EOF = Token.EOF
    COMMA=1
    LPAREN=2
    RPAREN=3
    LBRACE=4
    RBRACE=5
    LSQUARE=6
    RSQUARE=7
    COLON=8
    ID=9
    WS=10
    ESC_LITERAL=11

    def __init__(self, input:TokenStream, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.13.0")
        self._interp = ParserATNSimulator(self, self.atn, self.decisionsToDFA, self.sharedContextCache)
        self._predicates = None




    class PatternsContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def EOF(self):
            return self.getToken(PatternParser.EOF, 0)

        def pattern(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(PatternParser.PatternContext)
            else:
                return self.getTypedRuleContext(PatternParser.PatternContext,i)


        def COMMA(self, i:int=None):
            if i is None:
                return self.getTokens(PatternParser.COMMA)
            else:
                return self.getToken(PatternParser.COMMA, i)

        def getRuleIndex(self):
            return PatternParser.RULE_patterns

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPatterns" ):
                listener.enterPatterns(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPatterns" ):
                listener.exitPatterns(self)




    def patterns(self):

        localctx = PatternParser.PatternsContext(self, self._ctx, self.state)
        self.enterRule(localctx, 0, self.RULE_patterns)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 16
            self.pattern()
            self.state = 21
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==1:
                self.state = 17
                self.match(PatternParser.COMMA)
                self.state = 18
                self.pattern()
                self.state = 23
                self._errHandler.sync(self)
                _la = self._input.LA(1)

            self.state = 24
            self.match(PatternParser.EOF)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class PatternContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def nodePattern(self):
            return self.getTypedRuleContext(PatternParser.NodePatternContext,0)


        def edgePattern(self):
            return self.getTypedRuleContext(PatternParser.EdgePatternContext,0)


        def getRuleIndex(self):
            return PatternParser.RULE_pattern

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPattern" ):
                listener.enterPattern(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPattern" ):
                listener.exitPattern(self)




    def pattern(self):

        localctx = PatternParser.PatternContext(self, self._ctx, self.state)
        self.enterRule(localctx, 2, self.RULE_pattern)
        try:
            self.state = 28
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [2]:
                self.enterOuterAlt(localctx, 1)
                self.state = 26
                self.nodePattern()
                pass
            elif token in [6]:
                self.enterOuterAlt(localctx, 2)
                self.state = 27
                self.edgePattern()
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class NodePatternContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def LPAREN(self):
            return self.getToken(PatternParser.LPAREN, 0)

        def labelOrType(self):
            return self.getTypedRuleContext(PatternParser.LabelOrTypeContext,0)


        def RPAREN(self):
            return self.getToken(PatternParser.RPAREN, 0)

        def properties(self):
            return self.getTypedRuleContext(PatternParser.PropertiesContext,0)


        def getRuleIndex(self):
            return PatternParser.RULE_nodePattern

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNodePattern" ):
                listener.enterNodePattern(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNodePattern" ):
                listener.exitNodePattern(self)




    def nodePattern(self):

        localctx = PatternParser.NodePatternContext(self, self._ctx, self.state)
        self.enterRule(localctx, 4, self.RULE_nodePattern)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 30
            self.match(PatternParser.LPAREN)
            self.state = 31
            self.labelOrType()
            self.state = 33
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==4:
                self.state = 32
                self.properties()


            self.state = 35
            self.match(PatternParser.RPAREN)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class EdgePatternContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def LSQUARE(self):
            return self.getToken(PatternParser.LSQUARE, 0)

        def labelOrType(self):
            return self.getTypedRuleContext(PatternParser.LabelOrTypeContext,0)


        def RSQUARE(self):
            return self.getToken(PatternParser.RSQUARE, 0)

        def properties(self):
            return self.getTypedRuleContext(PatternParser.PropertiesContext,0)


        def getRuleIndex(self):
            return PatternParser.RULE_edgePattern

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterEdgePattern" ):
                listener.enterEdgePattern(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitEdgePattern" ):
                listener.exitEdgePattern(self)




    def edgePattern(self):

        localctx = PatternParser.EdgePatternContext(self, self._ctx, self.state)
        self.enterRule(localctx, 6, self.RULE_edgePattern)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 37
            self.match(PatternParser.LSQUARE)
            self.state = 38
            self.labelOrType()
            self.state = 40
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==4:
                self.state = 39
                self.properties()


            self.state = 42
            self.match(PatternParser.RSQUARE)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class LabelOrTypeContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def COLON(self):
            return self.getToken(PatternParser.COLON, 0)

        def name(self):
            return self.getTypedRuleContext(PatternParser.NameContext,0)


        def getRuleIndex(self):
            return PatternParser.RULE_labelOrType

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLabelOrType" ):
                listener.enterLabelOrType(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLabelOrType" ):
                listener.exitLabelOrType(self)




    def labelOrType(self):

        localctx = PatternParser.LabelOrTypeContext(self, self._ctx, self.state)
        self.enterRule(localctx, 8, self.RULE_labelOrType)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 44
            self.match(PatternParser.COLON)
            self.state = 45
            self.name()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class PropertiesContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def LBRACE(self):
            return self.getToken(PatternParser.LBRACE, 0)

        def RBRACE(self):
            return self.getToken(PatternParser.RBRACE, 0)

        def property_(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(PatternParser.PropertyContext)
            else:
                return self.getTypedRuleContext(PatternParser.PropertyContext,i)


        def COMMA(self, i:int=None):
            if i is None:
                return self.getTokens(PatternParser.COMMA)
            else:
                return self.getToken(PatternParser.COMMA, i)

        def getRuleIndex(self):
            return PatternParser.RULE_properties

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterProperties" ):
                listener.enterProperties(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitProperties" ):
                listener.exitProperties(self)




    def properties(self):

        localctx = PatternParser.PropertiesContext(self, self._ctx, self.state)
        self.enterRule(localctx, 10, self.RULE_properties)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 47
            self.match(PatternParser.LBRACE)
            self.state = 56
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            if _la==9 or _la==11:
                self.state = 48
                self.property_()
                self.state = 53
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                while _la==1:
                    self.state = 49
                    self.match(PatternParser.COMMA)
                    self.state = 50
                    self.property_()
                    self.state = 55
                    self._errHandler.sync(self)
                    _la = self._input.LA(1)



            self.state = 58
            self.match(PatternParser.RBRACE)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class PropertyContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def name(self):
            return self.getTypedRuleContext(PatternParser.NameContext,0)


        def getRuleIndex(self):
            return PatternParser.RULE_property

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterProperty" ):
                listener.enterProperty(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitProperty" ):
                listener.exitProperty(self)




    def property_(self):

        localctx = PatternParser.PropertyContext(self, self._ctx, self.state)
        self.enterRule(localctx, 12, self.RULE_property)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 60
            self.name()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class NameContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ID(self):
            return self.getToken(PatternParser.ID, 0)

        def ESC_LITERAL(self):
            return self.getToken(PatternParser.ESC_LITERAL, 0)

        def getRuleIndex(self):
            return PatternParser.RULE_name

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterName" ):
                listener.enterName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitName" ):
                listener.exitName(self)




    def name(self):

        localctx = PatternParser.NameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 14, self.RULE_name)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 62
            _la = self._input.LA(1)
            if not(_la==9 or _la==11):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx





