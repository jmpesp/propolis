// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! VM mainboard components. Every VM has a board, even if it has no other
//! peripherals.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::instance_spec::CpuidVendor;

/// An Intel 440FX-compatible chipset.
#[derive(
    Clone, Copy, Deserialize, Serialize, Debug, PartialEq, Eq, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct I440Fx {
    /// Specifies whether the chipset should allow PCI configuration space
    /// to be accessed through the PCIe extended configuration mechanism.
    pub enable_pcie: bool,
}

/// A kind of virtual chipset.
#[derive(
    Clone, Copy, Deserialize, Serialize, Debug, PartialEq, Eq, JsonSchema,
)]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    tag = "type",
    content = "value"
)]
pub enum Chipset {
    /// An Intel 440FX-compatible chipset.
    I440Fx(I440Fx),
}

impl Default for Chipset {
    fn default() -> Self {
        Self::I440Fx(I440Fx { enable_pcie: false })
    }
}

/// A set of CPUID values to expose to a guest.
#[derive(Clone, Deserialize, Serialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Cpuid {
    /// A list of CPUID leaves/subleaves and their associated values.
    ///
    /// Propolis servers require that each entry's `leaf` be unique and that it
    /// falls in either the "standard" (0 to 0xFFFF) or "extended" (0x8000_0000
    /// to 0x8000_FFFF) function ranges, since these are the only valid input
    /// ranges currently defined by Intel and AMD. See the Intel 64 and IA-32
    /// Architectures Software Developer's Manual (June 2024) Table 3-17 and the
    /// AMD64 Architecture Programmer's Manual (March 2024) Volume 3's
    /// documentation of the CPUID instruction.
    //
    // It would be nice if this were an associative collection type.
    // Unfortunately, the most natural keys for such a collection are
    // structs or tuples, and JSON doesn't allow objects to be used as
    // property names. Instead of converting leaf/subleaf pairs to and from
    // strings, just accept a flat Vec and have servers verify that e.g. no
    // leaf/subleaf pairs are duplicated.
    pub entries: Vec<CpuidEntry>,

    /// The CPU vendor to emulate.
    ///
    /// CPUID leaves in the extended range (0x8000_0000 to 0x8000_FFFF) have
    /// vendor-defined semantics. Propolis uses this value to determine
    /// these semantics when deciding whether it needs to specialize the
    /// supplied template values for these leaves.
    pub vendor: CpuidVendor,
}

/// A full description of a CPUID leaf/subleaf and the values it produces.
#[derive(
    Clone, Copy, Deserialize, Serialize, Debug, PartialEq, Eq, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct CpuidEntry {
    /// The leaf (function) number for this entry.
    pub leaf: u32,

    /// The subleaf (index) number for this entry, if it uses subleaves.
    pub subleaf: Option<u32>,

    /// The value to return in eax.
    pub eax: u32,

    /// The value to return in ebx.
    pub ebx: u32,

    /// The value to return in ecx.
    pub ecx: u32,

    /// The value to return in edx.
    pub edx: u32,
}

/// Flags that enable "simple" Hyper-V enlightenments that require no
/// feature-specific configuration.
//
// NOTE: This enum's variants should never have any associated data (note that
// the type doesn't use serde's `tag` and `content` attributes). If a future
// enlightenment requires associated data, it should be put into a
// `HyperVExtendedFeatures` struct (or similar), and the `HyperV` variant of
// `GuestHypervisorInterface` should be extended to `Option`ally include that
// struct.
#[derive(
    Clone,
    Deserialize,
    Serialize,
    Debug,
    JsonSchema,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum HyperVFeatureFlag {
    ReferenceTsc,
}

/// A hypervisor interface to expose to the guest.
#[derive(Clone, Deserialize, Serialize, Debug, JsonSchema, Default)]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    tag = "type",
    content = "value"
)]
pub enum GuestHypervisorInterface {
    /// Expose a bhyve-like interface ("bhyve bhyve " as the hypervisor ID in
    /// leaf 0x4000_0000 and no additional leaves or features).
    #[default]
    Bhyve,

    /// Expose a Hyper-V-compatible hypervisor interface with the supplied
    /// features enabled.
    HyperV { features: BTreeSet<HyperVFeatureFlag> },
}

impl GuestHypervisorInterface {
    fn is_default(&self) -> bool {
        matches!(self, Self::Bhyve)
    }
}

/// A VM's mainboard.
#[derive(Clone, Deserialize, Serialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Board {
    /// The number of virtual logical processors attached to this VM.
    pub cpus: u8,

    /// The amount of guest RAM attached to this VM.
    pub memory_mb: u64,

    /// The chipset to expose to guest software.
    pub chipset: Chipset,

    /// The hypervisor platform to expose to the guest. The default is a
    /// bhyve-compatible interface with no additional features.
    ///
    /// For compatibility with older versions of Propolis, this field is only
    /// serialized if it specifies a non-default interface.
    #[serde(
        default,
        skip_serializing_if = "GuestHypervisorInterface::is_default"
    )]
    pub guest_hv_interface: GuestHypervisorInterface,

    /// The CPUID values to expose to the guest. If `None`, bhyve will derive
    /// default values from the host's CPUID values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpuid: Option<Cpuid>,
    // TODO: Processor and NUMA topology.
}
