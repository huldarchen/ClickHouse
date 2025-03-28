
#include <iostream>
#include <string>

#include <IO/WriteBufferFromOStream.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>

#include <libfuzzer/libfuzzer_macro.h>

#include "out.pb.h"

void GenerateSentence(const Sentence&, std::string &, int);


DEFINE_BINARY_PROTO_FUZZER(const Sentence& main)
{
    static std::string input;
    input.reserve(4096);

    GenerateSentence(main, input, 0);
    if (input.size())
    {
        std::cout << input << std::endl;

        DB::ParserQueryWithOutput parser(input.data() + input.size());
        try
        {
            DB::ASTPtr ast
                = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0, DB::DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

            std::cerr << ast->formatWithSecretsOneLine();
            std::cerr << std::endl;
        }
        catch (...) {}

        input.clear();
    }
}
