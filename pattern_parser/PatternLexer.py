# Generated from Pattern.g4 by ANTLR 4.13.0
from antlr4 import *
from io import StringIO
import sys
if sys.version_info[1] > 5:
    from typing import TextIO
else:
    from typing.io import TextIO


def serializedATN():
    return [
        4,0,11,62,6,-1,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,
        6,7,6,2,7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,1,0,1,0,1,1,1,1,1,2,1,2,
        1,3,1,3,1,4,1,4,1,5,1,5,1,6,1,6,1,7,1,7,1,8,1,8,5,8,42,8,8,10,8,
        12,8,45,9,8,1,9,4,9,48,8,9,11,9,12,9,49,1,9,1,9,1,10,1,10,5,10,56,
        8,10,10,10,12,10,59,9,10,1,10,1,10,1,57,0,11,1,1,3,2,5,3,7,4,9,5,
        11,6,13,7,15,8,17,9,19,10,21,11,1,0,3,3,0,65,90,95,95,97,122,4,0,
        48,57,65,90,95,95,97,122,3,0,9,10,12,13,32,32,64,0,1,1,0,0,0,0,3,
        1,0,0,0,0,5,1,0,0,0,0,7,1,0,0,0,0,9,1,0,0,0,0,11,1,0,0,0,0,13,1,
        0,0,0,0,15,1,0,0,0,0,17,1,0,0,0,0,19,1,0,0,0,0,21,1,0,0,0,1,23,1,
        0,0,0,3,25,1,0,0,0,5,27,1,0,0,0,7,29,1,0,0,0,9,31,1,0,0,0,11,33,
        1,0,0,0,13,35,1,0,0,0,15,37,1,0,0,0,17,39,1,0,0,0,19,47,1,0,0,0,
        21,53,1,0,0,0,23,24,5,44,0,0,24,2,1,0,0,0,25,26,5,40,0,0,26,4,1,
        0,0,0,27,28,5,41,0,0,28,6,1,0,0,0,29,30,5,123,0,0,30,8,1,0,0,0,31,
        32,5,125,0,0,32,10,1,0,0,0,33,34,5,91,0,0,34,12,1,0,0,0,35,36,5,
        93,0,0,36,14,1,0,0,0,37,38,5,58,0,0,38,16,1,0,0,0,39,43,7,0,0,0,
        40,42,7,1,0,0,41,40,1,0,0,0,42,45,1,0,0,0,43,41,1,0,0,0,43,44,1,
        0,0,0,44,18,1,0,0,0,45,43,1,0,0,0,46,48,7,2,0,0,47,46,1,0,0,0,48,
        49,1,0,0,0,49,47,1,0,0,0,49,50,1,0,0,0,50,51,1,0,0,0,51,52,6,9,0,
        0,52,20,1,0,0,0,53,57,5,96,0,0,54,56,9,0,0,0,55,54,1,0,0,0,56,59,
        1,0,0,0,57,58,1,0,0,0,57,55,1,0,0,0,58,60,1,0,0,0,59,57,1,0,0,0,
        60,61,5,96,0,0,61,22,1,0,0,0,4,0,43,49,57,1,6,0,0
    ]

class PatternLexer(Lexer):

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    COMMA = 1
    LPAREN = 2
    RPAREN = 3
    LBRACE = 4
    RBRACE = 5
    LSQUARE = 6
    RSQUARE = 7
    COLON = 8
    ID = 9
    WS = 10
    ESC_LITERAL = 11

    channelNames = [ u"DEFAULT_TOKEN_CHANNEL", u"HIDDEN" ]

    modeNames = [ "DEFAULT_MODE" ]

    literalNames = [ "<INVALID>",
            "','", "'('", "')'", "'{'", "'}'", "'['", "']'", "':'" ]

    symbolicNames = [ "<INVALID>",
            "COMMA", "LPAREN", "RPAREN", "LBRACE", "RBRACE", "LSQUARE", 
            "RSQUARE", "COLON", "ID", "WS", "ESC_LITERAL" ]

    ruleNames = [ "COMMA", "LPAREN", "RPAREN", "LBRACE", "RBRACE", "LSQUARE", 
                  "RSQUARE", "COLON", "ID", "WS", "ESC_LITERAL" ]

    grammarFileName = "Pattern.g4"

    def __init__(self, input=None, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.13.0")
        self._interp = LexerATNSimulator(self, self.atn, self.decisionsToDFA, PredictionContextCache())
        self._actions = None
        self._predicates = None


