package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import sys.model.AvmovGuiaRemisionCab;
import sys.util.Service;
import sys.dao.GuiaRemisionDetalleDAO;
import sys.model.AvmovGuiaRemisionDet;

public class GuiaRemisionDetalleDAOImp implements GuiaRemisionDetalleDAO {

    @Override
    public List<AvmovGuiaRemisionDet> listarGuiasRemisionDetalle(AvmovGuiaRemisionCab guiaRemision) {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovGuiaRemisionDet guiaRemisionDetalle = null;
        List<AvmovGuiaRemisionDet> lista = new ArrayList<>();

        try {
            cn = Service.getConexion();
            String sql = "SELECT AVMOV_GUIA_REMISION_DET.Id_guia_remision_det, "
                    + "AVMOV_GUIA_REMISION_DET.Id_guia_remision_cab, "
                    + "AVMOV_GUIA_REMISION_DET.num_cantidad_producto, "
                    + "AVMOV_GUIA_REMISION_DET.cod_producto, "
                    + "AIMAR_Productos.Des_Ai_Produc, "
                    + "AVMOV_GUIA_REMISION_DET.cod_tipo_documento_origen, "
                    + "Agmae_tipos_documento.des_documento, "
                    + "AVMOV_GUIA_REMISION_DET.id_origen, "
                    + "AVMOV_GUIA_REMISION_DET.flg_estado, "
                    + "AVMOV_GUIA_REMISION_DET.fec_creacion, "
                    + "AVMOV_GUIA_REMISION_DET.hor_creacion, "
                    + "AVMOV_GUIA_REMISION_DET.cod_usuario_creacion, "
                    + "AVMOV_GUIA_REMISION_DET.fec_actualizacion, "
                    + "AVMOV_GUIA_REMISION_DET.hor_actualizacion, "
                    + "AVMOV_GUIA_REMISION_DET.cod_usuario_actualizacion, "
                    + "AVMOV_GUIA_REMISION_DET.cod_medida "
                    + "FROM Agmae_tipos_documento RIGHT JOIN (AIMAR_Productos RIGHT JOIN AVMOV_GUIA_REMISION_DET ON AIMAR_Productos.Cod_Ai_Produc = AVMOV_GUIA_REMISION_DET.cod_producto) ON Agmae_tipos_documento.cod_documento = AVMOV_GUIA_REMISION_DET.cod_tipo_documento_origen\n"
                    + "WHERE (((AVMOV_GUIA_REMISION_DET.Id_guia_remision_cab)=[?]));";
            ps = cn.prepareStatement(sql);
            ps.setInt(1, guiaRemision.getIdGuiaRemisionCab());
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                guiaRemisionDetalle = new AvmovGuiaRemisionDet();
                guiaRemisionDetalle.setIdGuiaRemisionDet(rs.getInt(1));
                guiaRemisionDetalle.setIdGuiaRemisionCab(rs.getInt(2));
                guiaRemisionDetalle.setNumCantidadProducto(rs.getInt(3));
                guiaRemisionDetalle.setCodProducto(rs.getString(4));
                guiaRemisionDetalle.setZnomProducto(rs.getString(5));
                guiaRemisionDetalle.setCodTipoDocumentoOrigen(rs.getString(6));
                guiaRemisionDetalle.setZnomTipoDocumentoOrigen(rs.getString(7));
                guiaRemisionDetalle.setIdOrigen(rs.getInt(8));
                guiaRemisionDetalle.setFlgEstado(rs.getInt(9));
                guiaRemisionDetalle.setFecCreacion(rs.getString(10));
                guiaRemisionDetalle.setHorCreacion(rs.getString(11));
                guiaRemisionDetalle.setCodUsuarioCreacion(rs.getInt(12));
                guiaRemisionDetalle.setFecActualizacion(rs.getString(13));
                guiaRemisionDetalle.setHorActualizacion(rs.getString(14));
                guiaRemisionDetalle.setCodUsuarioActualizacion(rs.getInt(15));
                guiaRemisionDetalle.setCodMedida(rs.getString(16));

                //se puede hacer en un solo campo pero para mayor entendimiento lo colocare asi
                guiaRemisionDetalle.setNuevo(false);
                guiaRemisionDetalle.setEditado(false);

                lista.add(guiaRemisionDetalle);
            }
            if (!existe) {
                guiaRemision = null;
                System.out.println("No se encontro datos");
            }
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
        return lista;

    }

    @Override
    public void newGuiaRemisionDetalle(AvmovGuiaRemisionDet guiaRemisionDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AVMOV_GUIA_REMISION_DET ("
                + "`Id_guia_remision_cab`, \n"
                + "`num_cantidad_producto`, \n"
                + "`cod_producto`, \n"
                + "`cod_medida`, "
                + "`cod_tipo_documento_origen`, \n"
                + "`id_origen`, \n"
                + "`flg_estado`, \n"
                + "`fec_creacion`, \n"
                + "`hor_creacion`, \n"
                + "`cod_usuario_creacion`) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?); ";
        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");
        guiaRemisionDetalle.setFecCreacion(formatoFecha.format(fechaActual));
        guiaRemisionDetalle.setHorCreacion(formatoHora.format(fechaActual));
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, guiaRemisionDetalle.getIdGuiaRemisionCab());
            ps.setInt(2, guiaRemisionDetalle.getNumCantidadProducto());
            ps.setString(3, guiaRemisionDetalle.getCodProducto());
            ps.setString(4, guiaRemisionDetalle.getCodMedida());
            ps.setString(5, guiaRemisionDetalle.getCodTipoDocumentoOrigen());
            ps.setInt(6, guiaRemisionDetalle.getIdOrigen());
            ps.setInt(7, guiaRemisionDetalle.getFlgEstado());
            ps.setString(8, guiaRemisionDetalle.getFecCreacion());
            ps.setString(9, guiaRemisionDetalle.getHorCreacion());
            ps.setInt(10, guiaRemisionDetalle.getCodUsuarioCreacion());

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
    public void updateGuiaRemisionDetalle(AvmovGuiaRemisionDet guiaRemisionDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AVMOV_GUIA_REMISION_DET SET "
                + "Id_guia_remision_cab= ?,\n"
                + "num_cantidad_producto= ?,\n"
                + "cod_producto= ?,\n"
                + "cod_medida=?, "
                + "cod_tipo_documento_origen= ?,\n"
                + "id_origen= ?,\n"
                + "flg_estado= ?,\n"
                + "fec_actualizacion= ?,\n"
                + "hor_actualizacion= ?,\n"
                + "cod_usuario_actualizacion= ? "
                + "WHERE Id_guia_remision_det= ?; ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        guiaRemisionDetalle.setFecActualizacion(formatoFecha.format(fechaActual));
        guiaRemisionDetalle.setHorActualizacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, guiaRemisionDetalle.getIdGuiaRemisionCab());
            ps.setInt(2, guiaRemisionDetalle.getNumCantidadProducto());
            ps.setString(3, guiaRemisionDetalle.getCodProducto());
            ps.setString(4, guiaRemisionDetalle.getCodMedida());
            ps.setString(5, guiaRemisionDetalle.getCodTipoDocumentoOrigen());
            ps.setInt(6, guiaRemisionDetalle.getIdOrigen());
            ps.setInt(7, guiaRemisionDetalle.getFlgEstado());
            ps.setString(8, guiaRemisionDetalle.getFecActualizacion());
            ps.setString(9, guiaRemisionDetalle.getHorActualizacion());
            ps.setInt(10, guiaRemisionDetalle.getCodUsuarioActualizacion());
            ps.setInt(11, guiaRemisionDetalle.getIdGuiaRemisionCab());

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
    public void deleteGuiaRemisionDetalle(AvmovGuiaRemisionDet guiaRemisionDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AVMOV_GUIA_REMISION_DET WHERE Id_guia_remision_det=?;";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, guiaRemisionDetalle.getIdGuiaRemisionDet());
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

    public java.sql.Date convertJavaDateToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }

}
