The vr-runner package provides a lightweight pipeline framework.

Runner pipelines
----------------
The runner pipelines are usually named as **run-something**. This document uses
**run-mpileup** as an example, but the usage is the same for all runners. 

For the impatient
-----------------
The minimum commands sufficient to install and run the pipelines is:

    # Get the code
    git clone git://github.com/VertebrateResequencing/vr-runner.git

    # Create a config file, edit, and run
    run-mpileup +sampleconf > my.conf
    vim my.conf
    run-mpileup +config my.conf +loop 600 -o outdir

Important: see also the section about setting environment variables below!

In more detail
--------------
The Runner.pm pipelines have two types of options. The options prefixed by "+" are common to all runners, the options prefixed by "-" are pipeline specific. For a complete list of the options run with -h, the output of which is shown at the bottom of this page

    run-mpileup -h

The pipelines can be run in a daemon mode (turned on by supplying the "**+loop**" option) or they can do one iteration only and stop ("**+loop**" option not given). The latter is suitable for execution from cron. Because the runner script itself consumes close to zero CPU time and memory, it can be run on farm head nodes. In this mode, it is convenient to run it from screen. For details about how to use screen see the manual pages (man screen), but generally knowing the following is enough:

  * Launch a new screen by executing the command **screen**
  * Type enter and run whatever commands you like. (In our case it will be the run-mpileup command.)
  * Detach from the screen by pressing **ctrl+a** followed by **d**
  * At any time one can reattach to a running screen by executing **screen -r**

If the farm has multiple head nodes, note that the screen lives on a concrete computer and therefore one has to log into the exact same machine to access it. One can simultaneously run as many screens as necessary.

Note that it is not necessary (nor recommended) to submit the runner scripts to the farm as they take little CPU and memory. Nonetheless, if the pipeline is run this way, the exit status is 111 when successfully completed. 

Setting environment variables
-----------------------------
The **vr-wrapper** script can be used to set environment variables without having
to change user's profile. It may look like this:

    #!/bin/bash
    export PATH="$HOME/git/vr-runner/scripts:$PATH"
    export PERL5LIB="$HOME/git/vr-runner/modules:$PERL5LIB"

Save the config in the file, say **~/.vrw/runners** and use like this:

    vr-wrapper ~/.vrw/runners run-mpileup +config my.conf +loop 600 -o outdir


Chaining multiple runners
-------------------------
Multi step pipelines can be created by chaining multiple runners and other commands via **run-runners**. The program waits for a new project to appear in an input dropbox directory, where it is picked up and automatically processesede. Once completed, the results are moved in an output dropbox directory and an email is sent to notify the user. 

The pipeline can be run in the daemon mode or from cron:

    # Run from the command line
    run-runners -v -c chain.gtcheck.conf -l +300
    
    # Run from cron
    */5 *  *   *   *     vr-wrapper ~/.vrw/runners 'run-runners -v -c config.conf -L config.lock'

Example config files can be found in the *misc* directory of the distribution. The projects are small text files which define the input data, they may look like this:

    # Where to send notification about job completion
    email: someone@somewhere.org 
    
    # Frequency (in seconds) with which to remind about failed jobs. This is to avoid
    # too many emails in your mailbox
    err_period: 3600
    
    # Any value given as "$(var_name)" in the runner's config can be overriden
    # The default value can be given as "$(var_name:default)"
    maxjobs: 200

Copy the project file in the input dropbox directory and **run-runners** will do the rest. Note that files starting with "**.**" (dot) or ending with "**~**" (tilde) will be ignored!


Runner options
--------------

    +help                   Summary of commands
    +config <file>          Configuration file
    +debug <file1> <file2>  Run the freezed object <file1> overriding with keys from <file2>
    +js <platform>          Job scheduler (lowercase allowed): LSF (bswitch), LSFCR (BLCR) [LSF]
    +kill                   Kill all running jobs
    +local                  Do not submit jobs to LSF, but run serially
    +lock <file>            Exit if another instance is already running
    +loop <int>             Run in daemon mode with <int> seconds sleep intervals
    +mail <address>         Email when the runner finishes
    +maxjobs <int>          Maximum number of simultaneously running jobs
    +nocache                When checking for finished files, do not rely on cached database and 
                               check again
    +retries <int>          Maximum number of retries. When negative, the runner eventually skips
                               the task rather than exiting completely. [1]
    +run <file> <id>        Run the freezed object created by spawn
    +sampleconf             Print a working configuration example
    +show <file>            Print the content of the freezed object created by spawn
    +silent                 Decrease verbosity of the Runner module

Frequently (and not so frequently) asked questions
--------------------------------------------------
<dl>
<dt>How to rerun a task</dt>
<dd>The pipelines know that a task finished by checking the existence of checkpoint files. In order to reduce the number of stat calls, the pipeline keeps a cache of finished tasks. Therefore, to rerun a task, it is not enough to remove the specific checkpoin file, one has to tell the pipeline to ignore the cached status: run the pipeline as usual, but add the <b>+nocache</b> option.
</dd>

<dt>How to skip a job</dt>
<dd>If a task keeps failing, the pipeline can be told to skip the offending task by setting negative value of the <b>+retries</b> option. With negative value of +retries, a skip file with the ".s" suffix is created and the job is reported as finished. The skip files are cleaned automatically when +retries is set to a positive value. A non cleanable variant is a force skip ".fs" file which is never cleaned by the pipeline and is created/removed manually by the user. When .fs is cleaned by the user, the pipeline must be run with +nocache in order to notice the change.
</dd>

<dt>Kill all running jobs</dt>
<dd>When something goes wrong and all jobs must be killed, the <b>+kill</b> option does the trick.
</dd>
</dl>
