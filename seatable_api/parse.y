%{
package seatable_api
import "fmt"
%}
 
%union {
    Op string
    Value string
    Rows interface{}
}
 
%type <Rows> merge
%type <Rows> filter
%type <Value> factor
 
%token <Value> QUOTE_STRING STRING
%token <Op> AND OR EQUAL NOT_EQUAL GTE GT LTE LT LIKE 
 
%%
merge:
     filter
    {
        $$=$1
    }
     |merge AND filter
    {
        $$=parser.Merge ($1, $2, $3)
    }
     |merge OR filter
    {
        $$=parser.Merge ($1, $2, $3)
    };
filter: factor EQUAL factor
    {
        $$=parser.Filter($1, $2, $3)
    }
    | factor NOT_EQUAL factor
    {
        $$=parser.Filter($1, $2, $3)
    }
    | factor GTE factor
    {
        $$=parser.Filter($1, $2, $3)
    }
    | factor GT factor
    {
        $$=parser.Filter($1, $2, $3)
    }
    | factor LTE factor
    {
        $$=parser.Filter($1, $2, $3)
    }
    | factor LT factor
    {
        $$=parser.Filter($1, $2, $3)
    }
    | factor LIKE factor
    {
        $$=parser.Filter($1, $2, $3)
    };
factor:QUOTE_STRING
    {
        $$=$1
    }
      |STRING
    {
        $$=$1
    };
%%
