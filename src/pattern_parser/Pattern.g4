grammar Pattern;

patterns
    : (pattern (COMMA pattern)*) EOF
    ;

pattern
    : nodePattern | edgePattern
    ;

nodePattern
    : LPAREN labelOrType properties? RPAREN
    ;

edgePattern
    : LSQUARE labelOrType properties? RSQUARE
    ;

labelOrType
    : (COLON name)
    ;

properties
    : LBRACE (property (COMMA property)*)? RBRACE
    ;

property
    : name
    ;

name
    : ID
    | ESC_LITERAL
    ;

COMMA : ',' ;
LPAREN : '(' ;
RPAREN : ')' ;
LBRACE : '{' ;
RBRACE : '}' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
COLON : ':' ;

ID: [a-zA-Z_][a-zA-Z_0-9]* ;
WS: [ \t\n\r\f]+ -> skip ;

ESC_LITERAL     : '`' .*? '`';
