package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import sys.dao.EmpresaTransporteDAO;
import sys.model.AimarEmpresaTransporte;
import sys.util.Service;

public class EmpresaTransporteDAOImp implements EmpresaTransporteDAO {

    @Override
    public List<AimarEmpresaTransporte> listarEmpresaTransporte() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarEmpresaTransporte empTransporte = null;
        List<AimarEmpresaTransporte> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AIMAR_EMPRESA_TRANSPORTE.cod_emptransporte, "
                    + "AIMAR_EMPRESA_TRANSPORTE.ruc_emptransporte, "
                    + "AIMAR_EMPRESA_TRANSPORTE.nom_emptransporte, "
                    + "AIMAR_EMPRESA_TRANSPORTE.num_telefono "
                    + "FROM AIMAR_EMPRESA_TRANSPORTE;";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                empTransporte = new AimarEmpresaTransporte();
                empTransporte.setCodEmptransporte(rs.getInt(1));
                empTransporte.setRucEmptransporte(rs.getString(2));
                empTransporte.setNomEmptransporte(rs.getString(3));
                empTransporte.setNumTelefono(rs.getString(4));

                lista.add(empTransporte);
            }
            if (!existe) {
                empTransporte = null;
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

}
