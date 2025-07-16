# Changelog

All notable changes to ZVPG will be documented in this file.

## [2.0.0] - 2025-07-16

### 🚀 Major Changes - Container-Based Architecture

This is a major architectural overhaul that introduces container-based PostgreSQL management instead of direct `pg_ctl` usage.

### ✨ Added

- **Container Runtime Support**: Added support for Docker, Podman, and nerdctl
- **User-Managed Configuration**: PostgreSQL configuration files now stored in `~/.zvpg/` directory
- **Flexible Container Images**: Configurable PostgreSQL base images
- **Enhanced Isolation**: Each branch runs in its own container with dedicated networking
- **Configuration Templates**: Included template files for easy setup
- **Migration Guide**: Comprehensive migration documentation for v1.x users

### 🔧 Changed

- **PostgreSQL Management**: Replaced direct `pg_ctl` usage with container orchestration
- **Configuration Structure**: Added new configuration options for container runtime
- **Port Management**: Enhanced port management with container-aware allocation
- **Branch Lifecycle**: Branches now manage container lifecycle instead of PostgreSQL processes
- **Health Checks**: Updated health checks to use container status and `pg_isready`

### 🗑️ Removed

- **Direct PostgreSQL Dependencies**: No longer requires PostgreSQL server binaries on host
- **Manual Configuration Management**: No longer modifies PostgreSQL configuration files directly
- **Socket-based Connections**: Removed Unix socket support in favor of TCP connections

### 📋 Configuration Changes

#### New Configuration Options:
- `containerRuntime`: Choose between "docker", "podman", or "nerdctl"
- `pgBaseImage`: Specify PostgreSQL container image (default: "postgres:17")
- `pgConfigDir`: Directory for PostgreSQL configuration files (default: "~/.zvpg")
- `pgHbaPath`: Path to pg_hba.conf file
- `pgIdentPath`: Path to pg_ident.conf file
- `pgConfPath`: Path to postgresql.conf file

#### Modified Behavior:
- Branch creation automatically starts PostgreSQL containers
- Branch deletion properly cleans up containers
- Port allocation includes container name mapping
- Status commands show container information

### 🔄 Migration Path

Users upgrading from v1.x should:
1. Stop all existing branches
2. Install a container runtime (Docker, Podman, or nerdctl)
3. Set up the `~/.zvpg/` configuration directory
4. Update configuration to specify container runtime
5. Test the new container-based setup

### 🐛 Bug Fixes

- Fixed port conflict detection with container awareness
- Improved cleanup of abandoned PostgreSQL processes
- Enhanced error handling for container operations
- Better resource management for long-running branches

### 📚 Documentation

- Updated README with container-based architecture diagrams
- Added troubleshooting section for container issues
- Created comprehensive configuration reference
- Added migration guide for v1.x users
- Included example configuration files

### 🔒 Security Improvements

- Container-based isolation provides better security boundaries
- User-managed configuration files reduce privilege escalation risks
- Network isolation through container networking
- Configurable authentication through pg_hba.conf templates

### ⚡ Performance Improvements

- Faster branch creation with container orchestration
- Reduced overhead from direct PostgreSQL management
- Better resource utilization through container limits
- Improved cleanup of idle resources

### 🧪 Testing

- Updated test suite for container-based operations
- Added container runtime compatibility tests
- Enhanced integration tests for multi-runtime support
- Improved error scenario testing

---

## [1.x] - 2025-07-14

Previous versions used direct PostgreSQL management with `pg_ctl`. See git history for detailed changes in the 1.x series.

### Notable v1.x Features:
- Direct ZFS snapshot and branch management
- PostgreSQL process management via `pg_ctl`
- Configuration file manipulation
- Unix socket support
- Direct filesystem operations

---

## Migration Notes

### Breaking Changes from v1.x to v2.0:

1. **Container Runtime Required**: Must install Docker, Podman, or nerdctl
2. **Configuration Format**: New configuration options added
3. **Connection Method**: TCP connections only (no Unix sockets)
4. **Port Range**: Default port range changed to 6001-6099
5. **PostgreSQL Dependencies**: Host PostgreSQL server no longer required

### Compatibility:

- ZFS snapshots and branches from v1.x are fully compatible
- Existing branch data can be used with container-based instances
- Configuration migration is required but data migration is not needed

### Support:

- v1.x is now deprecated but will receive security updates for 1 hour
- v2.0 is the recommended version for new installations
- Migration support available through GitHub issues
