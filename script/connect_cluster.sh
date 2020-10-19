#!/usr/bin/expect
set server [lindex $argv 0]
set timeout 3
spawn ssh cs4224c@xcnc$server.comp.nus.edu.sg
expect "*password*"
send "#SiGL9FC\r"
interact