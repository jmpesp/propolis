// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper functions for generating Windows guest OS adaptations.

use super::{CommandSequence, CommandSequenceEntry, GuestOsKind};

const CYGWIN_CMD: &str = "C:\\cygwin\\cygwin.bat\r";

/// Prepends a `reset` command to the shell command supplied in `cmd`. Windows
/// versions that drive a VT100 terminal can use this to try to force Windows to
/// clear and redraw the entire screen before displaying the command's output.
/// Without this, Windows may not render the post-output command prompt if the
/// post-command terminal state happens to place a prompt at a location that
/// already had onen pre-command.
pub(super) fn prepend_reset_to_shell_command(cmd: &str) -> String {
    format!("reset && {cmd}")
}

/// Emits the login seqeunce for the given `guest`, which must be one of the
/// Windows guest OS flavors.
///
/// This login sequence assumes the following:
///
/// - Cygwin is installed to C:\cygwin and can be launched by invoking
///   C:\cygwin\cygwin.bat.
/// - The local administrator account is enabled with password `0xide#1Fan`.
pub(super) fn get_login_sequence_for<'a>(
    guest: GuestOsKind,
) -> CommandSequence<'a> {
    assert!(matches!(
        guest,
        GuestOsKind::WindowsServer2016
            | GuestOsKind::WindowsServer2019
            | GuestOsKind::WindowsServer2022
    ));

    let mut commands = vec![
        // Look for `BdsDxe:` as a sign that we're actually seeing a fresh boot.
        // This is not terribly important in the case of a first boot, but in a
        // case such as logging out and waiting for reboot, exiting a cmd.exe
        // session causes Windows to redraw its previous screen - everything
        // past `Computer is booting, ...` below.
        //
        // A test that tries to boot and wait for a new login sequence would
        // then incorrectly identify the already-booted VM as the freshly-booted
        // OS it was waiting for, log in again, and at some point later finally
        // actually reboot.
        //
        // At least on Windows Server 2022, there is an XML prelude that is
        // printed to COM1 that we could look for here, but check for `BdsDxe: `
        // instead as that comes from OVMF and will be consistent regardless of
        // guest OS version.
        CommandSequenceEntry::wait_for("BdsDxe: loading "),
        CommandSequenceEntry::wait_for(
            "Computer is booting, SAC started and initialized.",
        ),
        CommandSequenceEntry::wait_for(
            "EVENT: The CMD command is now available.",
        ),
        CommandSequenceEntry::wait_for("SAC>"),
        CommandSequenceEntry::write_str("cmd"),
        CommandSequenceEntry::wait_for("Channel: Cmd0001"),
        CommandSequenceEntry::wait_for("SAC>"),
        CommandSequenceEntry::write_str("ch -sn Cmd0001"),
        CommandSequenceEntry::wait_for(
            "Use any other key to view this channel.",
        ),
        CommandSequenceEntry::write_str(""),
        CommandSequenceEntry::wait_for("Username:"),
        CommandSequenceEntry::write_str("Administrator"),
        CommandSequenceEntry::wait_for("Domain  :"),
        CommandSequenceEntry::write_str(""),
        CommandSequenceEntry::wait_for("Password:"),
        CommandSequenceEntry::write_str("0xide#1Fan"),
    ];

    // Earlier Windows Server versions' serial console-based command prompts
    // default to trying to drive a VT100 terminal themselves instead of
    // emitting characters and letting the recipient display them in whatever
    // style it likes. This only happens once the command prompt has been
    // activated, so only switch buffering modes after entering credentials.
    if matches!(
        guest,
        GuestOsKind::WindowsServer2016 | GuestOsKind::WindowsServer2019
    ) {
        commands.extend([
            CommandSequenceEntry::ChangeSerialConsoleBuffer(
                crate::serial::BufferKind::Vt80x24,
            ),
            // These versions also like to debounce keystrokes, so set a delay
            // between repeated characters to try to avoid this. This is a very
            // conservative delay to try to avoid test flakiness; fortunately,
            // it only applies when typing the same character multiple times in
            // a row.
            CommandSequenceEntry::SetRepeatedCharacterDebounce(
                std::time::Duration::from_millis(1500),
            ),
        ]);
    }

    commands.extend([
        // There appears (from observing Windows test reliability) to be some
        // kind of race at command prompt startup that can cause characters to
        // be eaten if they're typed too quickly after the command prompt
        // session launches. To get around this, try to send "serial console ok"
        // strings until one of them gets echoed back correctly or the entire
        // boot times out.
        CommandSequenceEntry::wait_for("C:\\Windows\\system32>"),
        CommandSequenceEntry::EstablishConsistentEcho {
            send: "echo serial console ok\\n\r\n".into(),
            expect: "serial console ok".into(),
            timeout: std::time::Duration::from_millis(250),
        },
        // Make sure there's a clean command prompt after establishing the echo.
        CommandSequenceEntry::write_str("cls\r"),
        CommandSequenceEntry::wait_for("C:\\Windows\\system32>"),
    ]);

    // Keep Cygwin from wrapping lines unexpectedly on Windows Server 2022 by
    // maximizing the effective console size before launching Cygwin. This just
    // confuses matters on Server 2016 and 2019, so on those guests just launch
    // Cygwin directly.
    if let GuestOsKind::WindowsServer2022 = guest {
        commands.push(CommandSequenceEntry::write_str(format!(
            "mode con cols=9999 lines=9999 && {CYGWIN_CMD}",
        )));
    } else {
        commands.push(CommandSequenceEntry::write_str(CYGWIN_CMD));
    }

    commands.extend([
        CommandSequenceEntry::wait_for("$ "),
        // Tweak the command prompt so that it appears on a single line with
        // no leading newlines.
        CommandSequenceEntry::write_str("PS1='$ '"),
    ]);

    CommandSequence(commands)
}
