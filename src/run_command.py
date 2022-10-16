import subprocess


def runcmd(cmd, verbose = False, *args, **kwargs):
    '''
    Runs the @cmd in a subprocess and returns the standard output as a string
    Use verbose=True to forward the standard output and standard error to the console
    '''

    process = subprocess.Popen(
        cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        text = True,
        shell = True
    )
    std_out, std_err = process.communicate()
    if verbose:
        print(std_out.strip(), std_err)
    return std_out.strip()

