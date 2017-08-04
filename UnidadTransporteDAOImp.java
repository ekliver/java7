/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import sys.dao.UnidadTransporteDAO;
import sys.model.AgmaeArea;
import sys.model.AimarUnidadTransporte;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class UnidadTransporteDAOImp implements UnidadTransporteDAO {

    @Override
    public List<AimarUnidadTransporte> listarUnidadTransporte() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarUnidadTransporte uT = null;
        List<AimarUnidadTransporte> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AIMAR_Unidad_Transporte.Pla_udt, "
                    + "AIMAR_Unidad_Transporte.Mar_udt, "
                    + "AIMAR_Unidad_Transporte.Nco_udt "
                    + "FROM AIMAR_Unidad_Transporte;";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                uT = new AimarUnidadTransporte();

                uT.setPlaca(rs.getString(1));
                uT.setMarca(rs.getString(2));
                uT.setConstancia(rs.getString(3));

                lista.add(uT);
            }
            if (!existe) {
                uT = null;
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
