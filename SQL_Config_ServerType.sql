USE [master]
GO
EXEC [master].sys.sp_addextendedproperty @name=N'Graphite_ha', @value=N'standalone.' 
GO
EXEC [master].sys.sp_addextendedproperty @name=N'Graphite_Prefix', @value=N'dc1.servertype1.server_subtype1.' 
GO
