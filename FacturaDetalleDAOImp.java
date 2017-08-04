package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import sys.dao.FacturaDetalleDAO;
import sys.model.AvmovFacturaNdCab;
import sys.model.AvmovFacturaNdDet;
import sys.model.AvmovGuiaRemisionCab;
import sys.model.AvmovGuiaRemisionDet;
import sys.model.AvmovMovNotaDespachoCab;
import sys.util.Service;

public class FacturaDetalleDAOImp implements FacturaDetalleDAO {

    @Override
    public List<AvmovFacturaNdDet> obtenerListaFacturaDetallePorIdFactura(AvmovFacturaNdCab factura) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovFacturaNdDet facturaDetalle = null;
        List<AvmovFacturaNdDet> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_Factura_ND_DET.idFacturaDet, "
                    + "AVMOV_Factura_ND_DET.idFacturaCab, "
                    + "AVMOV_Factura_ND_DET.idNotaDespachoDet, "
                    + "AVMOV_Factura_ND_DET.ruc_companyia, "
                    + "AVMOV_Factura_ND_DET.num_factura, "
                    + "AVMOV_Factura_ND_DET.cod_producto, "
                    + "AVMOV_Factura_ND_DET.num_cntd_producto, "
                    + "AVMOV_Factura_ND_DET.cod_medida, "
                    + "AVMOV_Factura_ND_DET.num_precio_unitario, "
                    + "AVMOV_Factura_ND_DET.cod_moneda_origen, "
                    + "AVMOV_Factura_ND_DET.precio_origen, "
                    + "AVMOV_Factura_ND_DET.num_importe_producto, "
                    + "AVMOV_Factura_ND_DET.fec_creacion, "
                    + "AVMOV_Factura_ND_DET.hor_creacion, "
                    + "AVMOV_Factura_ND_DET.cod_usuario_creacion, "
                    + "AVMOV_Factura_ND_DET.fec_actualizacion, "
                    + "AVMOV_Factura_ND_DET.hor_actualizacion, "
                    + "AVMOV_Factura_ND_DET.cod_usuario_actualizacion, "
                    + "AIMAR_Productos.Des_Ai_Produc "
                    + "FROM AIMAR_Productos RIGHT JOIN AVMOV_Factura_ND_DET ON (AIMAR_Productos.ruc_companyia = AVMOV_Factura_ND_DET.ruc_companyia) AND (AIMAR_Productos.Cod_Ai_Produc = AVMOV_Factura_ND_DET.cod_producto)\n"
                    + "WHERE (((AVMOV_Factura_ND_DET.idFacturaCab)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setInt(1, factura.getIdFacturaCab());

            rs = ps.executeQuery();

            while (rs.next()) {
                existe = true;
                facturaDetalle = new AvmovFacturaNdDet();
                facturaDetalle.setIdFacturaDet(rs.getInt(1));
                facturaDetalle.setIdFacturaCab(rs.getInt(2));
                facturaDetalle.setIdNotaDespachoDet(rs.getInt(3));
                facturaDetalle.setRucCompanyia(rs.getString(4));
                facturaDetalle.setNumFactura(rs.getString(5));
                facturaDetalle.setCodProducto(rs.getString(6));
                facturaDetalle.setNumCntdProducto(rs.getInt(7));
                facturaDetalle.setCodMedida(rs.getString(8));
                facturaDetalle.setNumPrecioUnitario(rs.getDouble(9));
                facturaDetalle.setCodMonedaOrigen(rs.getString(10));
                facturaDetalle.setNumPrecioOrigen(rs.getDouble(11));
                facturaDetalle.setNumImporteProducto(rs.getDouble(12));
                facturaDetalle.setFecCreacion(rs.getString(13));
                facturaDetalle.setHorCreacion(rs.getString(14));
                facturaDetalle.setCodUsuarioCreacion(rs.getInt(15));
                facturaDetalle.setFecActualizacion(rs.getString(16));
                facturaDetalle.setHorActualizacion(rs.getString(17));
                facturaDetalle.setCodUsuarioActualizacion(rs.getInt(18));
                facturaDetalle.setZnomProducto(rs.getString(19));
                //se puede hacer en un solo campo pero para mayor entendimiento lo colocare asi
                facturaDetalle.setNuevo(false);
                facturaDetalle.setEditado(false);

                lista.add(facturaDetalle);

            }

            if (!existe) {
                facturaDetalle = null;
                System.out.println("No se encontro datos");
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return lista;
    }

    @Override
    public List<AvmovFacturaNdDet> listarFacturaDetalleSinGR(AvmovFacturaNdCab factura, List<AvmovGuiaRemisionDet> guiaRemisionDetalle) {
        String idFacturaNoVa = "0";
        String idNotaDespachoNoVa = "0";
        if (guiaRemisionDetalle.size() > 0 && guiaRemisionDetalle != null) {
            for (AvmovGuiaRemisionDet item : guiaRemisionDetalle) {
                if (item.getCodTipoDocumentoOrigen().equals("01")) {
                    idFacturaNoVa = idFacturaNoVa + "," + item.getIdOrigen();
                    idNotaDespachoNoVa = idNotaDespachoNoVa + "," + item.getIdOrigen();
                } else if (item.getCodTipoDocumentoOrigen().equals("09")) {
                    idNotaDespachoNoVa = idNotaDespachoNoVa + "," + item.getIdOrigen();
                }
            }
        }
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovFacturaNdDet facturaDetalle = null;
        List<AvmovFacturaNdDet> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_Factura_ND_DET.idFacturaDet, "
                    + "AVMOV_Factura_ND_DET.idFacturaCab, "
                    + "AVMOV_Factura_ND_DET.idNotaDespachoDet, "
                    + "AVMOV_Factura_ND_DET.ruc_companyia, "
                    + "AVMOV_Factura_ND_DET.cod_producto, "
                    + "AIMAR_Productos.Des_Ai_Produc, "
                    + "AVMOV_Factura_ND_DET.num_cntd_producto, "
                    + "AVMOV_Factura_ND_DET.cod_medida, "
                    + "AVMOV_Factura_ND_DET.flg_estado_guia_remision "
                    + "FROM AIMAR_Productos RIGHT JOIN AVMOV_Factura_ND_DET ON (AIMAR_Productos.ruc_companyia = AVMOV_Factura_ND_DET.ruc_companyia) AND (AIMAR_Productos.Cod_Ai_Produc = AVMOV_Factura_ND_DET.cod_producto) "
                    + "WHERE (((AVMOV_Factura_ND_DET.flg_estado_guia_remision)<>1) AND (AVMOV_Factura_ND_DET.idFacturaCab=?) "
                    + "AND (AVMOV_Factura_ND_DET.idFacturaDet NOT IN(" + idFacturaNoVa + ")) AND (AVMOV_Factura_ND_DET.idNotaDespachoDet NOT IN(" + idNotaDespachoNoVa + ")) );";
            ps = cn.prepareStatement(sql);
            ps.setInt(1, factura.getIdFacturaCab());
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                facturaDetalle = new AvmovFacturaNdDet();
                facturaDetalle.setIdFacturaDet(rs.getInt(1));
                facturaDetalle.setIdFacturaCab(rs.getInt(2));
                facturaDetalle.setIdNotaDespachoDet(rs.getInt(3));
                facturaDetalle.setRucCompanyia(rs.getString(4));
                facturaDetalle.setCodProducto(rs.getString(5));
                facturaDetalle.setZnomProducto(rs.getString(6));
                facturaDetalle.setNumCntdProducto(rs.getInt(7));
                facturaDetalle.setCodMedida(rs.getString(8));
                facturaDetalle.setFlgEstadoGuiaRemision(rs.getInt(9));

                lista.add(facturaDetalle);

            }

            if (!existe) {
                facturaDetalle = null;
                System.out.println("No se encontro datos");
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return lista;
    }

    @Override
    public List<AvmovFacturaNdDet> obtenerListaFacturaDetallePorND(AvmovMovNotaDespachoCab notaDespacho) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovFacturaNdDet facturaDetalle = null;
        List<AvmovFacturaNdDet> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_MovNotaDespachoDet.id_MovValeProducto AS idMovDet, AVMOV_MovNotaDespachoDet.ruc_Companyia AS rucCompanyia, AVMOV_MovNotaDespachoDet.cod_producto AS codProducto, AIMAR_Productos.Des_Ai_Produc AS nomProducto, AVMOV_MovNotaDespachoDet.ctd_Movimiento AS cantProducto, AIMAR_Productos.Cod_Medida AS uniMedida, (IIf(Z_Producto_Precio.cod_moneda='1',Z_Producto_Precio.Precio_Producto,Z_Producto_Precio.Precio_Producto*(SELECT val_moneda_a FROM AVMOV_Conversor_Moneda WHERE (cod_moneda_de='2' AND cod_moneda_a='1' )))) AS puProducto, Z_Producto_Precio.cod_moneda AS codMoneda, Z_Producto_Precio.Precio_Producto AS precioOrigen, (puProducto*cantProducto) AS impProducto, AVMOV_MovNotaDespachoDet.id_MovValeCab\n"
                    + "FROM (AIMAR_Productos RIGHT JOIN AVMOV_MovNotaDespachoDet ON (AIMAR_Productos.Cod_Ai_Produc = AVMOV_MovNotaDespachoDet.cod_producto) AND (AIMAR_Productos.ruc_companyia = AVMOV_MovNotaDespachoDet.ruc_Companyia)) LEFT JOIN Z_Producto_Precio ON AVMOV_MovNotaDespachoDet.cod_producto = Z_Producto_Precio.Cod_Producto \n"
                    + "WHERE (((AVMOV_MovNotaDespachoDet.flg_estado_factura)=0) AND ((AVMOV_MovNotaDespachoDet.ruc_Companyia)=?) AND ((AVMOV_MovNotaDespachoDet.id_MovValeCab)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setString(1, notaDespacho.getRucCompanyia());
            ps.setInt(2, notaDespacho.getIdMovValeCab());

            rs = ps.executeQuery();

            while (rs.next()) {
                existe = true;
                facturaDetalle = new AvmovFacturaNdDet();

                facturaDetalle.setIdNotaDespachoDet(rs.getInt("idMovDet"));
                facturaDetalle.setRucCompanyia(rs.getString("rucCompanyia"));
                facturaDetalle.setCodProducto(rs.getString("codProducto"));
                facturaDetalle.setZnomProducto(rs.getString("nomProducto"));
                facturaDetalle.setNumCntdProducto(rs.getInt("cantProducto"));
                facturaDetalle.setCodMedida(rs.getString("uniMedida"));
                facturaDetalle.setNumPrecioUnitario(rs.getDouble("puProducto"));
                facturaDetalle.setCodMonedaOrigen(rs.getString("codMoneda"));
                facturaDetalle.setNumPrecioOrigen(rs.getDouble("precioOrigen"));
                facturaDetalle.setNumImporteProducto(rs.getDouble("impProducto"));
                //se puede hacer en un solo campo pero para mayor entendimiento lo colocare asi
                facturaDetalle.setNuevo(true);
                facturaDetalle.setEditado(false);

                lista.add(facturaDetalle);

            }

            if (!existe) {
                facturaDetalle = null;
                System.out.println("No se encontro datos");
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return lista;
    }

    @Override
    public List<AvmovFacturaNdDet> obtenerListaProductoPendientesPorIdRel(AvmovMovNotaDespachoCab notaDespacho, List<AvmovFacturaNdDet> listaFacturaDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovFacturaNdDet facturaDetalle = null;
        List<AvmovFacturaNdDet> lista = new ArrayList<>();

        String itemsExistentes = "0";
        if (listaFacturaDetalle.size() > 0) {
            for (AvmovFacturaNdDet item : listaFacturaDetalle) {
                itemsExistentes += "," + item.getIdNotaDespachoDet();
            }
        }

        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_MovNotaDespachoDet.id_MovValeProducto AS idMovDet, "
                    + "AVMOV_MovNotaDespachoDet.ruc_Companyia AS rucCompanyia, "
                    + "AVMOV_MovNotaDespachoDet.cod_producto AS codProducto, AIMAR_Productos.Des_Ai_Produc AS nomProducto, "
                    + "AVMOV_MovNotaDespachoDet.ctd_Movimiento AS cantProducto, AIMAR_Productos.Cod_Medida AS uniMedida, "
                    + "(IIf(Z_Producto_Precio.cod_moneda='1',Z_Producto_Precio.Precio_Producto,Z_Producto_Precio.Precio_Producto*(SELECT val_moneda_a FROM AVMOV_Conversor_Moneda WHERE (cod_moneda_de='2' AND cod_moneda_a='1' )))) AS puProducto, "
                    + "Z_Producto_Precio.cod_moneda AS codMoneda, "
                    + "Z_Producto_Precio.Precio_Producto AS precioOrigen, (puProducto*cantProducto) AS impProducto "
                    + "FROM (AIMAR_Productos RIGHT JOIN AVMOV_MovNotaDespachoDet ON (AIMAR_Productos.Cod_Ai_Produc = AVMOV_MovNotaDespachoDet.cod_producto) AND (AIMAR_Productos.ruc_companyia = AVMOV_MovNotaDespachoDet.ruc_Companyia)) LEFT JOIN Z_Producto_Precio ON AVMOV_MovNotaDespachoDet.cod_producto = Z_Producto_Precio.Cod_Producto "
                    + "WHERE (((AVMOV_MovNotaDespachoDet.flg_estado_factura)=0) AND ((AVMOV_MovNotaDespachoDet.ruc_Companyia)=?) AND ((AVMOV_MovNotaDespachoDet.id_MovValeCab)=[?])  AND ((AVMOV_MovNotaDespachoDet.id_MovValeProducto) Not In ( " + itemsExistentes + " )) );";

            ps = cn.prepareStatement(sql);

            ps.setString(1, notaDespacho.getRucCompanyia());
            ps.setInt(2, notaDespacho.getIdMovValeCab());

            rs = ps.executeQuery();

            while (rs.next()) {
                existe = true;
                facturaDetalle = new AvmovFacturaNdDet();

                facturaDetalle.setIdNotaDespachoDet(rs.getInt("idMovDet"));
                facturaDetalle.setRucCompanyia(rs.getString("rucCompanyia"));
                facturaDetalle.setCodProducto(rs.getString("codProducto"));
                facturaDetalle.setZnomProducto(rs.getString("nomProducto"));
                facturaDetalle.setNumCntdProducto(rs.getInt("cantProducto"));
                facturaDetalle.setCodMedida(rs.getString("uniMedida"));
                facturaDetalle.setNumPrecioUnitario(rs.getDouble("puProducto"));
                facturaDetalle.setCodMonedaOrigen(rs.getString("codMoneda"));
                facturaDetalle.setNumPrecioOrigen(rs.getDouble("precioOrigen"));
                facturaDetalle.setNumImporteProducto(rs.getDouble("impProducto"));
                //se puede hacer en un solo campo pero para mayor entendimiento lo colocare asi
                facturaDetalle.setNuevo(true);
                facturaDetalle.setEditado(false);

                lista.add(facturaDetalle);

            }

            if (!existe) {
                notaDespacho = null;
                System.out.println("No se encontro datos");
            }
//            Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }

        return lista;
    }

    @Override
    public void newFacturaDetalle(AvmovFacturaNdDet facturaDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AVMOV_Factura_ND_DET ("
                + "`idFacturaCab`, "
                + "`idNotaDespachoDet`, "
                + "`ruc_companyia`, "
                + "`num_factura`, "
                + "`cod_producto`, "
                + "`num_cntd_producto`, "
                + "`cod_medida`, "
                + "`num_precio_unitario`, "
                + "`cod_moneda_origen`, "
                + "`precio_origen`, "
                + "`num_importe_producto`, "
                + "`fec_creacion`, "
                + "`hor_creacion`, "
                + "`cod_usuario_creacion`) "
                + "VALUES("
                + "?,?,?,?,?,"
                + "?,?,?,?,?,"
                + "?,?,?,?); ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        facturaDetalle.setFecCreacion(formatoFecha.format(fechaActual));
        facturaDetalle.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, facturaDetalle.getIdFacturaCab());
            ps.setInt(2, facturaDetalle.getIdNotaDespachoDet());
            ps.setString(3, facturaDetalle.getRucCompanyia());
            ps.setString(4, facturaDetalle.getNumFactura());
            ps.setString(5, facturaDetalle.getCodProducto());
            ps.setInt(6, facturaDetalle.getNumCntdProducto());
            ps.setString(7, facturaDetalle.getCodMedida());
            ps.setDouble(8, facturaDetalle.getNumPrecioUnitario());
            ps.setString(9, facturaDetalle.getCodMonedaOrigen());
            ps.setDouble(10, facturaDetalle.getNumPrecioOrigen());
            ps.setDouble(11, facturaDetalle.getNumImporteProducto());
            ps.setString(12, facturaDetalle.getFecCreacion());
            ps.setString(13, facturaDetalle.getHorCreacion());
            ps.setInt(14, facturaDetalle.getCodUsuarioCreacion());

            ps.executeUpdate();
//              Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    @Override
    public void updateFacturaDetalle(AvmovFacturaNdDet facturaDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AVMOV_Factura_ND_DET SET "
                + "idFacturaCab=?, \n"
                + "IdMovAlmDet=?, \n"
                + "ruc_companyia=?, \n"
                + "num_factura=?, \n"
                + "cod_producto=?, \n"
                + "num_cntd_producto=?, \n"
                + "cod_medida=?, \n"
                + "num_precio_unitario=?, \n"
                + "cod_moneda_origen=?, \n"
                + "precio_origen=?, \n"
                + "num_importe_producto=?, \n"
                + "fec_actualizacion=?, \n"
                + "hor_actualizacion=?, \n"
                + "cod_usuario_actualizacion=?, "
                + "WHERE idFacturaDet=?; ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        facturaDetalle.setFecCreacion(formatoFecha.format(fechaActual));
        facturaDetalle.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, facturaDetalle.getIdFacturaCab());
            ps.setInt(2, facturaDetalle.getIdNotaDespachoDet());
            ps.setString(3, facturaDetalle.getRucCompanyia());
            ps.setString(4, facturaDetalle.getNumFactura());
            ps.setString(5, facturaDetalle.getCodProducto());
            ps.setInt(6, facturaDetalle.getNumCntdProducto());
            ps.setString(7, facturaDetalle.getCodMedida());
            ps.setDouble(8, facturaDetalle.getNumPrecioUnitario());
            ps.setString(9, facturaDetalle.getCodMonedaOrigen());
            ps.setDouble(10, facturaDetalle.getNumPrecioOrigen());
            ps.setDouble(11, facturaDetalle.getNumImporteProducto());
            ps.setString(12, facturaDetalle.getFecActualizacion());
            ps.setString(13, facturaDetalle.getHorActualizacion());
            ps.setInt(14, facturaDetalle.getCodUsuarioActualizacion());
            ps.setInt(15, facturaDetalle.getIdFacturaDet());

            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
    }

    @Override
    public void deleteFacturaDetalle(AvmovFacturaNdDet facturaDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AVMOV_Factura_ND_DET "
                + "WHERE idFacturaDet=?; ";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, facturaDetalle.getIdFacturaDet());

            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
    }

}
