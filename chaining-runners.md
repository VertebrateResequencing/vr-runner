Chaining multiple runners
-------------------------
Multiple runners can be chained together using the `run-runners` pipeline manager. It runs as a daemon or from `cron` and waits for a new project to appear in a dropbox directory. After completion of the project, the results appear in output directory and the user is notified by an email. Multiple users can use the same dropbox setup to run different pipelines at the same time.

The following directory structure is used:

    prefix.conf .. config directory which contains pipeline definitions. 
                    Typically this is a symbolic link to the vr-runner/misc
                    install directory.  This is the only requirement, the rest
                    is created automatically by the pipeline
    prefix.in   .. dropbox input directory, drop your project here
    prefix.out  .. output directory, the result will appear here
    prefix.tmp  .. working directory

Each pipeline chain consists of a series of steps defined in the `chain.*.conf` and `step.*.conf` files. For examples take a look in the `misc` directory of this distribution. Configs files in this direcotry can be used directly as they are, just symlink them as described above in the description of `prefix.conf`. If the config files are well written, the user only needs to create a small project description and place it in the dropbox input directory:

    .config:         chain.gtcheck.conf
    .email:          someone\@somewhere.org
    mpileup/alns:    bams.list
    mpileup/fa_ref:  human_g1k_v37.fasta

The project descriptions are small text files which define the input data.
All keys prefixed with a dot are specific to the project, the rest are config
keys specific to one or multiple steps:

 
    # The pipeline config, known also as the "chain file". Note that in order to
    # avoid the execution of an arbitrary code passed by a malicious user, the config
    # must be a file name with any leading directory components removed. The file
    # must exist in the dropbox config directory:
    config: chain.pipeline.conf
 
    # The rest is optional and pipeline-specific. There are a few pipeline-wide 
    # options that are recognised by all pipelines, such as where to send email 
    # notifications about job completion and failures:
    email: someone@somewhere.org someone.else@somewhere.else.org
 
    # Frequency (in minutes) with which to remind about failed jobs, so that
    # the mailbox does not end up cluttered by emails. If not given, the default
    # of 60 minutes is used:
    err_period: 60
 
    # Any key in a runner's config file can be overriden. For example, if a pipeline
    # chain includes a runner step named "step_name" which recognises the config 
    # key "var_name", the default key can be overriden as:
    step_name/var_name: new value

    # If multiple steps share the same config key, a comma-separated list of
    # steps can be given or leave out the step name to set the key in all steps:
    step1,step2/var_name: new value
    var_name: new value
 
    # Also substrings in a runner's config file can be expanded. For example,
    # if the config contains the following key-value pair
    #   key => 'some $(value)',
    # the variable "$(value)" can be replaced with "new value":
    step_name/value: new value

    # In order to preserve compactness of project descriptions yet allowing flexibility,
    # the runner's config files can use default values. For example, if the config
    # contains the following key-value pair
    #   key => 'some $(value:sensible default)',
    # the project description file can override the value but in case it does not,
    # the key will be expanded as follows
    #   key => 'some sensible default',

    # To undefine a pre-defined key, pass "undef"
    step/key: undef      # undefine a key
    step/key: 'undef'    # pass string "undef"

    # See the misc/*conf files for real-life examples.

 
The pipeline chain can be run from the command line:
 
    run-runners -v -d prefix            # one iteration
    run-runners -v -d prefix -l 300     # loop with sleep cycles of 300 seconds
 
or from cron:
 
    */5 *  *   *   *     vr-wrapper ~/.vrw/runners 'run-runners -i -v -d prefix -L config.lock'    
 
The vr-wrapper scripts can be used to set environment variables without having to change user's profile. It may look like this:
 
    #!/bin/bash
    export PATH="$HOME/git/vr-runner/scripts:$PATH"
    export PERL5LIB="$HOME/git/vr-runner/modules:$PERL5LIB"


