# The checker embeds the compiler, whose VM only registers callbacks such as
# querySetting, getEnv and staticExec under `nimcore` (as nimsuggest does).
# Without it, compile-time code in dependencies (e.g. bearssl >= 0.2.14) fails
# with "implemented in the vmops".
switch("define", "nimcore")
